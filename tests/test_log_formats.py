import json
import pytest

from spark_forensics.eventlog import parse_event_log

EVENT = {
    "Event": "SparkListenerTaskEnd", "Stage ID": 1, "Stage Attempt ID": 0,
    "Task End Reason": {"Reason": "Success"},
    "Task Info": {"Task ID": 1, "Index": 0, "Attempt": 0, "Launch Time": 0,
                  "Finish Time": 10, "Failed": False, "Killed": False},
    "Task Metrics": {"Executor Run Time": 9, "JVM GC Time": 0},
}


def test_rolling_directory_is_read_in_order(tmp_path):
    d = tmp_path / "eventlog_v2_app"
    d.mkdir()
    second = {**EVENT, "Task Info": {**EVENT["Task Info"], "Task ID": 2, "Index": 1}}
    (d / "events_2_app").write_text(json.dumps(second) + "\n")
    (d / "events_1_app").write_text(json.dumps(EVENT) + "\n")
    (d / "appstatus_app").write_text("")
    (stage,) = parse_event_log(d)
    assert [t.task_id for t in stage.tasks] == [1, 2]


def test_zstd_log_is_read(tmp_path):
    zstandard = pytest.importorskip("zstandard")
    path = tmp_path / "events_1_app.zstd"
    path.write_bytes(zstandard.ZstdCompressor().compress((json.dumps(EVENT) + "\n").encode()))
    (stage,) = parse_event_log(path)
    assert len(stage.tasks) == 1


def test_unsupported_codec_fails_clearly(tmp_path):
    path = tmp_path / "events_1_app.lz4"
    path.write_bytes(b"x")
    with pytest.raises(ValueError, match="not supported"):
        parse_event_log(path)
