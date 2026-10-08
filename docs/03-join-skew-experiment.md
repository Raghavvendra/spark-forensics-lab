# 3. Experiment — Join Skew

## Question

What does data skew look like in Spark at task level, and how can event-log evidence distinguish skew from a generic "slow Spark job" explanation?

## Workload design

The workload should intentionally concentrate a large fraction of rows on one or a small number of join keys while keeping the remaining keys relatively distributed.

The experiment should contain:

- a large fact-like dataset
- a smaller dimension-like dataset
- an equality join on the skewed key
- a fixed logical query across all comparison runs

## Baseline conditions

For the baseline, the experiment should disable mechanisms that could automatically hide the behavior being studied. Record the exact settings in the run notes.

Example settings to consider for a controlled baseline:

```text
spark.sql.autoBroadcastJoinThreshold = -1
spark.sql.adaptive.enabled = false
```

These settings must be recorded with the actual run rather than assumed.

## Evidence to collect

Capture the stage that contains the expensive shuffle/join and record:

- number of tasks
- task duration distribution
- shuffle read bytes
- shuffle read records
- spill bytes
- JVM GC time

## Expected forensic signal

If the skew hypothesis is correct, one or a few tasks should receive much more shuffle data/records than an approximately uniform partition would, and those tasks should also be strong duration outliers.

The important point is not merely that one task is slow. A slow task is an observation. Skew becomes a stronger diagnosis when **task duration and partition data share the same imbalance**.

## Comparison runs

### Run A — Baseline

Known skew + controlled baseline settings.

### Run B — AQE

Same workload and query with Adaptive Query Execution enabled.

Record whether the execution plan and task distribution change.

### Run C — Salting

Use a salting strategy to distribute the hot key across multiple partitions.

The output semantics must remain equivalent.

## Results table

| Run | Runtime | Max task | Median task | Max/median | Shuffle read | Spill | GC | Diagnosis |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Baseline | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |
| AQE | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |
| Salting | TODO | TODO | TODO | TODO | TODO | TODO | TODO | TODO |

## Conclusion template

> The baseline showed [observed task imbalance]. The slowest task processed [observed data/records] compared with the rest of the stage. This supports/does not support the skew hypothesis because [reason]. After [intervention], [specific metric] changed from [A] to [B]. The main remaining uncertainty is [uncertainty].
