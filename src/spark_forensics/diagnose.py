from __future__ import annotations

import statistics
from dataclasses import dataclass

from .models import Diagnosis, StageSummary


@dataclass(frozen=True)
class DiagnosisConfig:
    max_to_median_ratio: float = 3.0
    shuffle_vs_uniform_ratio: float = 2.5
    records_vs_uniform_ratio: float = 2.5
    spill_share_threshold: float = 0.10
    gc_share_threshold: float = 0.20


def _percentile_95(values: list[float]) -> float | None:
    if len(values) < 20:
        return None
    return statistics.quantiles(values, n=20, method="inclusive")[18]


def diagnose_stage(stage: StageSummary, config: DiagnosisConfig | None = None) -> list[Diagnosis]:
    config = config or DiagnosisConfig()
    if len(stage.tasks) < 2:
        return []

    durations = [task.duration_ms for task in stage.tasks]
    shuffle_reads = [task.shuffle_read_bytes for task in stage.tasks]
    shuffle_records = [task.shuffle_read_records for task in stage.tasks]
    gc_times = [task.jvm_gc_time_ms for task in stage.tasks]
    spilled = [task.spill_memory_bytes + task.spill_disk_bytes for task in stage.tasks]

    median_ms = statistics.median(durations)
    max_ms = max(durations)
    p95_ms = _percentile_95(durations)
    total_shuffle = sum(shuffle_reads)
    largest_shuffle = max(shuffle_reads)
    total_shuffle_records = sum(shuffle_records)
    largest_shuffle_records = max(shuffle_records)
    total_input = sum(task.input_bytes for task in stage.tasks)
    total_run = sum(task.executor_run_time_ms for task in stage.tasks)
    total_gc = sum(gc_times)
    total_spill = sum(spilled)

    max_to_median = max_ms / median_ms if median_ms else float("inf")
    shuffle_share = largest_shuffle / total_shuffle if total_shuffle else 0.0
    records_share = (
        largest_shuffle_records / total_shuffle_records
        if total_shuffle_records
        else 0.0
    )
    expected_share = 1.0 / len(stage.tasks)
    shuffle_vs_uniform = shuffle_share / expected_share if expected_share else 0.0
    records_vs_uniform = records_share / expected_share if expected_share else 0.0
    gc_share = total_gc / total_run if total_run else 0.0
    slowest_task = max(stage.tasks, key=lambda task: task.duration_ms)
    slowest_task_gc_share = (
        slowest_task.jvm_gc_time_ms / slowest_task.executor_run_time_ms
        if slowest_task.executor_run_time_ms
        else 0.0
    )
    worst_task_gc_share = max(
        (
            task.jvm_gc_time_ms / task.executor_run_time_ms
            if task.executor_run_time_ms
            else 0.0
        )
        for task in stage.tasks
    )
    spill_denominator = total_input + total_shuffle
    spill_share = total_spill / spill_denominator if spill_denominator else 0.0

    results: list[Diagnosis] = []

    if (
        (total_shuffle > 0 or total_shuffle_records > 0)
        and max_to_median >= config.max_to_median_ratio
        and (
            shuffle_vs_uniform >= config.shuffle_vs_uniform_ratio
            or records_vs_uniform >= config.records_vs_uniform_ratio
        )
    ):
        evidence_source = []
        if shuffle_vs_uniform >= config.shuffle_vs_uniform_ratio and total_shuffle > 0:
            evidence_source.append("shuffle bytes")
        if records_vs_uniform >= config.records_vs_uniform_ratio and total_shuffle_records > 0:
            evidence_source.append("shuffle records")
        sources = " and ".join(evidence_source)
        explanation = (
            "The slowest task is a large time outlier, and one task carries much more "
            f"{sources} than a uniform partition would. "
            "That is evidence for data skew."
        )
        if spill_share >= config.spill_share_threshold:
            explanation += " Spill is also elevated, so memory pressure is a plausible consequence of the skew."
        results.append(
            Diagnosis(
                code="DATA_SKEW",
                evidence={
                    "task_count": len(stage.tasks),
                    "median_task_ms": round(median_ms, 2),
                    "p95_task_ms": None if p95_ms is None else round(p95_ms, 2),
                    "max_task_ms": round(max_ms, 2),
                    "max_to_median": round(max_to_median, 2),
                    "largest_partition_shuffle_share": round(shuffle_share, 3),
                    "shuffle_vs_uniform": round(shuffle_vs_uniform, 2),
                    "largest_partition_shuffle_record_share": round(records_share, 3),
                    "records_vs_uniform": round(records_vs_uniform, 2),
                    "spill_share_of_input_plus_shuffle": round(spill_share, 3),
                    "threshold_max_to_median": config.max_to_median_ratio,
                    "threshold_shuffle_vs_uniform": config.shuffle_vs_uniform_ratio,
                    "threshold_records_vs_uniform": config.records_vs_uniform_ratio,
                },
                explanation=explanation,
                rank=1,
            )
        )

    if total_spill > 0 and spill_share >= config.spill_share_threshold:
        results.append(
            Diagnosis(
                code="SPILL_PRESSURE",
                evidence={
                    "spill_bytes": total_spill,
                    "input_bytes": total_input,
                    "shuffle_read_bytes": total_shuffle,
                    "spill_share_of_input_plus_shuffle": round(spill_share, 3),
                    "threshold_spill_share": config.spill_share_threshold,
                },
                explanation=(
                    "The stage spilled a meaningful amount of data relative to its input plus shuffle bytes. "
                    "This points to memory pressure; it is a competing explanation for slow tasks "
                    "and can also be downstream of skew."
                ),
                rank=2,
            )
        )

    if slowest_task_gc_share >= config.gc_share_threshold:
        results.append(
            Diagnosis(
                code="GC_PRESSURE",
                evidence={
                    "jvm_gc_time_ms": total_gc,
                    "executor_run_time_ms": total_run,
                    "stage_gc_share_of_run_time": round(gc_share, 3),
                    "slowest_task_gc_share_of_run_time": round(slowest_task_gc_share, 3),
                    "worst_task_gc_share_of_run_time": round(worst_task_gc_share, 3),
                    "threshold_gc_share": config.gc_share_threshold,
                },
                explanation=(
                    "The slowest task spent a substantial fraction of its executor run time in JVM garbage collection. "
                    "That is a separate performance signal and should be checked before attributing all "
                    "of the slowdown to skew."
                ),
                rank=2,
            )
        )

    return sorted(results, key=lambda diagnosis: (diagnosis.rank, diagnosis.code))
