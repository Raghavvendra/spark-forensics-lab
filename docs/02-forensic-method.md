# 2. Forensic Method

Use the same sequence for every experiment.

## Step 1 — Define the suspected problem

Example:

> One join key occurs far more often than the others. I expect the corresponding shuffle partition to receive disproportionate work.

## Step 2 — Freeze the workload

Record:

- input data generation method
- row counts
- key distribution
- query
- Spark version
- important Spark configuration
- machine/runtime environment

Do not change several variables at once.

## Step 3 — Establish a baseline

Run the workload with the suspected problem intentionally present.

Save:

- final runtime
- Spark event log
- relevant Spark UI screenshots
- stage/task metrics

## Step 4 — Inspect task-level evidence

Start with the slowest stage.

Compare:

```text
max task duration / median task duration

largest partition shuffle / total shuffle

largest partition records / total records

spill / (input + shuffle)

gc time / executor run time
```

These ratios are easier to compare across different runs than raw numbers alone.

## Step 5 — Form a diagnosis

Do not write:

> Spark is slow because of skew.

Write:

> The slowest task is a large time outlier, and the same task carries a disproportionately large share of shuffle data/records. This is evidence consistent with data skew.

The distinction matters: the second statement identifies the evidence supporting the conclusion.

## Step 6 — Change one mechanism

Examples:

- enable AQE
- introduce salting
- change a join strategy
- change partitioning

Keep the logical result equivalent.

## Step 7 — Re-run and compare

Create a small comparison table:

| Metric | Baseline | Mitigation | Interpretation |
|---|---:|---:|---|
| Runtime | fill | fill | overall result |
| Max task duration | fill | fill | straggler severity |
| Median task duration | fill | fill | typical task |
| Max/median ratio | fill | fill | imbalance |
| Shuffle read | fill | fill | data movement |
| Spill | fill | fill | memory pressure |
| GC time | fill | fill | JVM pressure |

Do not invent values. Populate the table from the actual run.

## Step 8 — Write the conclusion

Use this structure:

> **Initial hypothesis:** ...
>
> **Evidence:** ...
>
> **Intervention:** ...
>
> **Observed change:** ...
>
> **Conclusion:** ...
>
> **Remaining uncertainty:** ...

That last line is important. Good forensic analysis states what the evidence does not prove.
