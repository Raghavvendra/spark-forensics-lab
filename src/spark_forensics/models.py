from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TaskSummary:
    task_id: int
    duration_ms: float
    executor_run_time_ms: float
    jvm_gc_time_ms: float = 0.0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    spill_memory_bytes: int = 0
    spill_disk_bytes: int = 0
    input_bytes: int = 0
    input_records: int = 0
    shuffle_read_records: int = 0


@dataclass(frozen=True)
class StageSummary:
    stage_id: int
    name: str
    tasks: tuple[TaskSummary, ...]
    stage_attempt_id: int = 0
    status: str = "completed"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Diagnosis:
    code: str
    evidence: dict[str, float | int | str]
    explanation: str
    rank: int = 1
