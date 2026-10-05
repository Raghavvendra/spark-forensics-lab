import json
from pathlib import Path

import pytest

from spark_forensics.eventlog import parse_event_log


def _task_event(
    stage_id,
    stage_attempt_id,
    task_id,
    index,
    launch,
    finish,
    remote_shuffle=0,
    local_shuffle=0,
    gc=0,
    spill=0,
    attempt=0,
    failed=False,
    killed=False,
    reason="Success",
):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": stage_attempt_id,
        "Task Type": "ShuffleMapTask",
        "Task Info": {
            "Task ID": task_id,
            "Index": index,
            "Attempt": attempt,
            "Launch Time": launch,
            "Finish Time": finish,
            "Failed": failed,
            "Killed": killed,
        },
        "Task End Reason": {"Reason": reason},
        "Task Metrics": {
            "Executor Run Time": finish - launch,
            "JVM GC Time": gc,
            "Memory Bytes Spilled": spill,
            "Disk Bytes Spilled": 0,
            "Shuffle Read Metrics": {
                "Remote Bytes Read": remote_shuffle,
                "Local Bytes Read": local_shuffle,
                "Total Records Read": 0,
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
            "Input Metrics": {"Bytes Read": 0, "Records Read": 0},
        },
    }


def test_real_spark_schema_fixture_parses():
    fixture = Path(__file__).parent / "fixtures" / "spark_official_event_log_excerpt.jsonl"
    stages = parse_event_log(fixture)

    assert len(stages) == 1
    assert stages[0].stage_id == 1
    assert stages[0].stage_attempt_id == 0
    assert stages[0].tasks[0].jvm_gc_time_ms == 11
    assert stages[0].tasks[0].input_bytes == 2_500_050_000


def test_shuffle_read_uses_remote_plus_local_bytes(tmp_path):
    path = tmp_path / "events.log"
    events = [
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 7, "Stage Attempt ID": 0, "Stage Name": "join"}},
        _task_event(7, 0, 1, 0, 100, 200, remote_shuffle=1200, local_shuffle=800),
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 7, "Stage Attempt ID": 0, "Stage Name": "join", "Failure Reason": None}},
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    stages = parse_event_log(path)

    assert stages[0].tasks[0].shuffle_read_bytes == 2000


def test_successful_stage_attempt_and_successful_task_attempt_are_selected(tmp_path):
    path = tmp_path / "events.log"
    events = [
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 1, "Stage Attempt ID": 0, "Stage Name": "stage"}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 1, "Stage Attempt ID": 1, "Stage Name": "stage"}},
        _task_event(1, 0, 10, 0, 100, 300, attempt=0),
        _task_event(1, 0, 11, 0, 200, 250, attempt=1),
        _task_event(1, 1, 12, 0, 300, 420, remote_shuffle=500),
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 1, "Stage Attempt ID": 0, "Failure Reason": "failed"}},
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 1, "Stage Attempt ID": 1, "Failure Reason": None}},
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    stages = parse_event_log(path)

    assert len(stages) == 1
    assert stages[0].stage_attempt_id == 1
    assert [task.task_id for task in stages[0].tasks] == [12]


def test_skips_truncated_final_line(tmp_path):
    path = tmp_path / "truncated.log"
    path.write_text(
        '{"Event":"SparkListenerLogStart","Spark Version":"4.2.0"}\n'
        '{"Event":"SparkListenerTaskEnd",',
        encoding="utf-8",
    )

    with pytest.warns(RuntimeWarning, match="truncated final"):
        stages = parse_event_log(path)

    assert stages == ()


def test_incomplete_stage_is_kept_and_flagged(tmp_path):
    path = tmp_path / "killed.log"
    events = [
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 9, "Stage Attempt ID": 0, "Stage Name": "running-stage"}},
        _task_event(9, 0, 21, 0, 100, 500, remote_shuffle=100),
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    stages = parse_event_log(path)

    assert len(stages) == 1
    assert stages[0].status == "incomplete"
    assert stages[0].stage_id == 9


def test_completed_successful_stage_is_preferred_over_an_incomplete_attempt(tmp_path):
    path = tmp_path / "retry.log"
    events = [
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 10, "Stage Attempt ID": 0, "Stage Name": "stage"}},
        _task_event(10, 0, 31, 0, 100, 300, remote_shuffle=100),
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 10, "Stage Attempt ID": 0, "Failure Reason": None}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 10, "Stage Attempt ID": 1, "Stage Name": "stage"}},
        _task_event(10, 1, 32, 0, 400, 900, remote_shuffle=500),
    ]
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    stages = parse_event_log(path)

    assert len(stages) == 1
    assert stages[0].stage_attempt_id == 0
    assert stages[0].status == "completed"


def test_task_records_are_parsed(tmp_path):
    path = tmp_path / "records.log"
    event = _task_event(11, 0, 41, 0, 100, 200, remote_shuffle=10, local_shuffle=5)
    event["Task Metrics"]["Shuffle Read Metrics"]["Total Records Read"] = 250
    event["Task Metrics"]["Input Metrics"]["Records Read"] = 400
    events = [
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 11, "Stage Attempt ID": 0, "Stage Name": "records"}},
        event,
    ]
    path.write_text("".join(json.dumps(item) + "\n" for item in events), encoding="utf-8")

    stages = parse_event_log(path)

    assert stages[0].tasks[0].shuffle_read_records == 250
    assert stages[0].tasks[0].input_records == 400
