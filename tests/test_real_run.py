from pathlib import Path

from spark_forensics.diagnose import diagnose_stage
from spark_forensics.eventlog import parse_event_log

FIXTURE = Path(__file__).parent / "fixtures" / "real_skew_stage2.jsonl"


def test_real_skewed_run_is_diagnosed_as_data_skew():
    """Integration test on a trimmed log from a real run of experiments/01_skew_workload.py."""
    (stage,) = parse_event_log(FIXTURE)
    assert stage.stage_id == 2 and len(stage.tasks) == 32 and stage.status == "completed"

    # The hot-key task read ~6.7 MB / ~809k records; typical tasks read ~0.1 MB / ~10k.
    hottest = max(stage.tasks, key=lambda t: t.shuffle_read_bytes)
    assert hottest.shuffle_read_bytes > 6_000_000
    assert hottest.shuffle_read_records > 800_000

    diagnosis = diagnose_stage(stage)[0]
    assert diagnosis.code == "DATA_SKEW"
    assert diagnosis.evidence["shuffle_vs_uniform"] > 15
    # Bytes understate row skew because identical keys compress well.
    assert diagnosis.evidence["records_vs_uniform"] > diagnosis.evidence["shuffle_vs_uniform"]
