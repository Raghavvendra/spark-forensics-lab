# Experiment 01 notes

## Question

Does the hot key create a task-time and shuffle-size outlier when broadcast and AQE are turned off?

## Baseline controls

`spark.sql.autoBroadcastJoinThreshold=-1`

`spark.sql.adaptive.enabled=false`

`spark.sql.shuffle.partitions=32`

These controls make the baseline easier to reason about. They are not production recommendations.

## Evidence used by the analyzer

The parser reads `SparkListenerTaskEnd` events and keeps task-level run time, shuffle read, spill, and JVM GC time.

For shuffle read, the event log contains `Remote Bytes Read` and `Local Bytes Read`; the parser adds those two fields.

For skew, the analyzer uses a task-time outlier plus either of two configurable concentration signals:

1. largest task's shuffle-byte share relative to the uniform share implied by task count
2. largest task's shuffle-record share relative to that uniform share

Spill is measured against input bytes plus shuffle-read bytes, so a read-heavy aggregation can still surface spill pressure without any shuffle input. JVM GC is checked at the slowest task as well as stage-wide context.

If a stage never reaches completion, it is retained and marked `incomplete` rather than discarded. That matters when investigating killed applications.

## Thresholds

The defaults are deliberately treated as heuristics, not Spark rules. They stay configurable until we have measurements from a real run.

## What is not claimed yet (updated: see docs/first-real-run.md)

The repository does not contain a fabricated skew result. The first real skew fixture should come from running `experiments/01_skew_workload.py`, checking the Spark UI, and then preserving the resulting event log (or a small representative excerpt) under `tests/fixtures/`.

That real fixture will become the integration test for the end-to-end skew diagnosis.
