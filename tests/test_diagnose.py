from spark_forensics.diagnose import DiagnosisConfig, diagnose_stage
from spark_forensics.models import StageSummary, TaskSummary


def test_detects_data_skew_and_mentions_spill():
    stage = StageSummary(
        stage_id=1,
        name="join",
        tasks=tuple(
            TaskSummary(i, 10_000 + i * 100, 9_500, shuffle_read_bytes=100, spill_memory_bytes=0)
            for i in range(31)
        )
        + (TaskSummary(31, 90_000, 80_000, shuffle_read_bytes=5_000, spill_memory_bytes=900),),
    )

    diagnoses = diagnose_stage(stage)
    assert [d.code for d in diagnoses][:2] == ["DATA_SKEW", "SPILL_PRESSURE"]
    assert "memory pressure" in diagnoses[0].explanation


def test_balanced_stage_has_no_diagnosis():
    stage = StageSummary(
        stage_id=2,
        name="aggregate",
        tasks=tuple(TaskSummary(i, 10_000 + i * 100, 9_500, shuffle_read_bytes=100) for i in range(32)),
    )
    assert diagnose_stage(stage) == []


def test_thresholds_are_configurable():
    stage = StageSummary(
        stage_id=3,
        name="join",
        tasks=tuple(TaskSummary(i, 10_000, 9_500, shuffle_read_bytes=100) for i in range(31))
        + (TaskSummary(31, 25_000, 24_000, shuffle_read_bytes=500),),
    )

    assert diagnose_stage(stage) == []
    config = DiagnosisConfig(max_to_median_ratio=2.0, shuffle_vs_uniform_ratio=2.0)
    assert diagnose_stage(stage, config)[0].code == "DATA_SKEW"


def test_gc_pressure_uses_the_slowest_task_not_only_the_stage_average():
    stage = StageSummary(
        stage_id=4,
        name="join",
        tasks=tuple(
            TaskSummary(i, 10_000, 10_000, jvm_gc_time_ms=100) for i in range(31)
        )
        + (TaskSummary(31, 90_000, 90_000, jvm_gc_time_ms=30_000),),
    )

    diagnoses = diagnose_stage(stage)
    assert [d.code for d in diagnoses] == ["GC_PRESSURE"]
    assert diagnoses[0].evidence["slowest_task_gc_share_of_run_time"] == 0.333


def test_spill_pressure_uses_input_plus_shuffle_when_no_shuffle():
    stage = StageSummary(
        stage_id=5,
        name="aggregate",
        tasks=tuple(
            TaskSummary(i, 10_000, 10_000, input_bytes=1_000_000, spill_memory_bytes=150_000)
            for i in range(32)
        ),
    )

    diagnoses = diagnose_stage(stage)
    assert [d.code for d in diagnoses] == ["SPILL_PRESSURE"]
    assert diagnoses[0].evidence["spill_share_of_input_plus_shuffle"] == 0.15


def test_records_can_trigger_skew_when_shuffle_bytes_are_balanced():
    stage = StageSummary(
        stage_id=6,
        name="join",
        tasks=tuple(
            TaskSummary(i, 10_000, 10_000, shuffle_read_records=100, shuffle_read_bytes=1_000)
            for i in range(31)
        )
        + (
            TaskSummary(
                31,
                80_000,
                75_000,
                shuffle_read_records=5_000,
                shuffle_read_bytes=1_000,
            ),
        ),
    )

    diagnoses = diagnose_stage(stage)

    assert diagnoses[0].code == "DATA_SKEW"
    assert diagnoses[0].evidence["records_vs_uniform"] > 2.5
