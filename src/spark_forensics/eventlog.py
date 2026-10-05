from __future__ import annotations

import gzip
import io
import json
import re
import warnings
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .models import StageSummary, TaskSummary


def _event_files(path: Path) -> list[Path]:
    """A rolling (v2) Spark log is a directory of events_<n>_<app> files, in order."""
    if not path.is_dir():
        return [path]
    files = [p for p in path.iterdir() if p.name.startswith("events_")]
    if not files:
        raise ValueError(f"No events_* files found in event-log directory {path}")

    def index(p: Path) -> int:
        match = re.match(r"events_(\d+)_", p.name)
        return int(match.group(1)) if match else 0

    return sorted(files, key=index)


def _open_text(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    if suffix in (".zstd", ".zst"):
        try:
            import zstandard
        except ImportError as exc:
            raise ValueError(
                f"{path.name} is zstd-compressed; install it with: pip install zstandard"
            ) from exc
        reader = zstandard.ZstdDecompressor().stream_reader(path.open("rb"))
        return io.TextIOWrapper(reader, encoding="utf-8")
    if suffix in (".lz4", ".snappy"):
        raise ValueError(f"{path.name}: {suffix} logs are not supported; decompress first")
    return path.open("r", encoding="utf-8")


def _iter_lines(path: Path):
    for file in _event_files(path):
        with _open_text(file) as handle:
            yield from handle


@contextmanager
def _line_source(path: Path):
    lines = _iter_lines(path)
    try:
        yield lines
    finally:
        lines.close()


def _first(mapping: dict[str, Any], *keys: str, default: Any = 0) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return default


def _number(metrics: dict[str, Any], section: str, *keys: str) -> int:
    block = metrics.get(section) or {}
    value = _first(block, *keys, default=0)
    return int(value or 0)


def _shuffle_read_bytes(metrics: dict[str, Any]) -> int:
    """Spark event logs expose remote/local shuffle bytes, not REST-style totals."""
    block = metrics.get("Shuffle Read Metrics") or {}
    remote = int(block.get("Remote Bytes Read", 0) or 0)
    local = int(block.get("Local Bytes Read", 0) or 0)
    return remote + local


def _task_summary(event: dict[str, Any]) -> tuple[tuple[int, int, int], dict[str, Any]] | None:
    task_info = event.get("Task Info") or {}
    task_metrics = event.get("Task Metrics") or {}
    stage_id = int(event.get("Stage ID", -1))
    stage_attempt_id = int(event.get("Stage Attempt ID", 0))
    task_id = int(task_info.get("Task ID", -1))
    index = int(task_info.get("Index", -1))
    attempt = int(task_info.get("Attempt", 0))

    reason = (event.get("Task End Reason") or {}).get("Reason", "")
    failed = bool(task_info.get("Failed", False))
    killed = bool(task_info.get("Killed", False))
    successful = not failed and not killed and reason == "Success"

    if stage_id < 0 or task_id < 0 or index < 0 or not successful:
        return None

    launch_time = int(_first(task_info, "Launch Time", default=0) or 0)
    finish_time = int(_first(task_info, "Finish Time", default=launch_time) or launch_time)
    key = (stage_id, stage_attempt_id, index)
    summary = {
        "stage_id": stage_id,
        "stage_attempt_id": stage_attempt_id,
        "task_id": task_id,
        "attempt": attempt,
        "duration_ms": max(0, finish_time - launch_time),
        "executor_run_time_ms": int(
            _first(task_metrics, "Executor Run Time", default=0) or 0
        ),
        "jvm_gc_time_ms": int(
            _first(task_metrics, "JVM GC Time", default=0) or 0
        ),
        "shuffle_read_bytes": _shuffle_read_bytes(task_metrics),
        "shuffle_write_bytes": _number(
            task_metrics, "Shuffle Write Metrics", "Shuffle Bytes Written"
        ),
        "spill_memory_bytes": int(
            _first(task_metrics, "Memory Bytes Spilled", default=0) or 0
        ),
        "spill_disk_bytes": int(
            _first(task_metrics, "Disk Bytes Spilled", default=0) or 0
        ),
        "input_bytes": _number(task_metrics, "Input Metrics", "Bytes Read"),
        "input_records": _number(task_metrics, "Input Metrics", "Records Read"),
        "shuffle_read_records": _number(
            task_metrics, "Shuffle Read Metrics", "Total Records Read"
        ),
    }
    return key, summary


def parse_event_log(path: Path) -> tuple[StageSummary, ...]:
    """Parse a Spark JSON-lines event log without loading it all into memory."""
    stage_names: dict[tuple[int, int], str] = {}
    stage_status: dict[tuple[int, int], str] = {}
    latest_tasks: dict[tuple[int, int, int], dict[str, Any]] = {}

    with _line_source(path) as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                # Only forgive a bad *last* line (killed app); anything later is corruption.
                if not any(rest.strip() for rest in handle):
                    warnings.warn(
                        f"Skipping truncated final event-log line {line_number}: {exc.msg}",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    continue
                raise ValueError(f"Invalid JSON at line {line_number}: {exc}") from exc

            kind = event.get("Event")
            if kind == "SparkListenerStageSubmitted":
                info = event.get("Stage Info") or {}
                stage_id = int(info.get("Stage ID", event.get("Stage ID", -1)))
                attempt_id = int(info.get("Stage Attempt ID", event.get("Stage Attempt ID", 0)))
                key = (stage_id, attempt_id)
                stage_names[key] = str(info.get("Stage Name", f"stage-{stage_id}"))
                stage_status[key] = "incomplete"
            elif kind == "SparkListenerStageCompleted":
                info = event.get("Stage Info") or {}
                stage_id = int(info.get("Stage ID", event.get("Stage ID", -1)))
                attempt_id = int(info.get("Stage Attempt ID", event.get("Stage Attempt ID", 0)))
                key = (stage_id, attempt_id)
                failure_reason = info.get("Failure Reason")
                stage_status[key] = "completed" if failure_reason in (None, "") else "failed"
            elif kind == "SparkListenerTaskEnd":
                result = _task_summary(event)
                if result is None:
                    continue
                key, summary = result
                previous = latest_tasks.get(key)
                if previous is None or summary["attempt"] >= previous["attempt"]:
                    latest_tasks[key] = summary

    by_stage_attempt: dict[tuple[int, int], list[TaskSummary]] = defaultdict(list)
    for item in latest_tasks.values():
        key = (item["stage_id"], item["stage_attempt_id"])
        by_stage_attempt[key].append(
            TaskSummary(
                task_id=item["task_id"],
                duration_ms=item["duration_ms"],
                executor_run_time_ms=item["executor_run_time_ms"],
                jvm_gc_time_ms=item["jvm_gc_time_ms"],
                shuffle_read_bytes=item["shuffle_read_bytes"],
                shuffle_write_bytes=item["shuffle_write_bytes"],
                spill_memory_bytes=item["spill_memory_bytes"],
                spill_disk_bytes=item["spill_disk_bytes"],
                input_bytes=item["input_bytes"],
                input_records=item["input_records"],
                shuffle_read_records=item["shuffle_read_records"],
            )
        )

    stage_attempts = set(stage_names) | set(by_stage_attempt) | set(stage_status)
    grouped_attempts: dict[int, list[int]] = defaultdict(list)
    for stage_id, attempt_id in stage_attempts:
        grouped_attempts[stage_id].append(attempt_id)

    selected: dict[int, int] = {}
    priority = {"completed": 2, "incomplete": 1, "failed": 0}
    for stage_id, attempts in grouped_attempts.items():
        attempts = sorted(attempts, key=lambda attempt: (priority.get(stage_status.get((stage_id, attempt), "incomplete"), 0), attempt), reverse=True)
        for attempt_id in attempts:
            if (stage_id, attempt_id) in by_stage_attempt:
                selected[stage_id] = attempt_id
                break
            if stage_status.get((stage_id, attempt_id)) == "incomplete":
                selected[stage_id] = attempt_id
                break

    stages: list[StageSummary] = []
    for stage_id, attempt_id in sorted(selected.items()):
        tasks = sorted(
            by_stage_attempt.get((stage_id, attempt_id), []),
            key=lambda task: task.task_id,
        )
        stages.append(
            StageSummary(
                stage_id=stage_id,
                stage_attempt_id=attempt_id,
                name=stage_names.get((stage_id, attempt_id), f"stage-{stage_id}"),
                tasks=tuple(tasks),
                status=stage_status.get((stage_id, attempt_id), "incomplete"),
            )
        )

    return tuple(stages)
