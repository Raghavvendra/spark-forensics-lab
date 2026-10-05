# Test fixtures

## real_skew_stage2.jsonl  (real run)
Trimmed from an actual run of `experiments/01_skew_workload.py`
(Spark 4.2.0, Java 21, Python 3.12, `local[4]`). Kept: the log-start event, stage 2
submitted/completed, and its 32 `SparkListenerTaskEnd` events. Only the bulky
`Accumulables` list was emptied; every `Task Metrics` field is untouched.
Used by `tests/test_real_run.py` as the end-to-end skew test.

## spark_official_event_log_excerpt.jsonl  (schema check)
Excerpt of `core/src/test/resources/spark-events/application_1553914137147_0018` from
the Apache Spark repository (Apache License 2.0). It only keeps the parser tied to the
schema Spark emits. It is not a performance result and not the skew experiment.
