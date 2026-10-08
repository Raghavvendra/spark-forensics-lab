# 6. Interview Notes — Spark Forensics

## 30-second explanation

> I built a small Spark forensics lab to practice diagnosing slow jobs from event-log evidence instead of relying only on runtime. The first experiment focuses on join skew. I intentionally create an uneven workload, inspect task-level shuffle, duration, spill and GC metrics, and then compare the baseline with mitigation strategies such as AQE and salting.

## Likely interviewer questions

### Why not just look at total runtime?

Because runtime identifies the symptom, not the mechanism. Task-level metrics can show whether work is unevenly distributed or whether memory/GC pressure is involved.

### Why compare max task time with the median?

A maximum alone can be noisy. Comparing it with the median tells us how much the slowest task deviates from the normal task population.

### Why use shuffle records as well as shuffle bytes?

Two partitions can have similar byte volumes but very different record counts. Record distribution provides a different view of imbalance.

### Does a slow task prove skew?

No. A slow task is only an observation. Skew is better supported when task duration outliers align with disproportionate partition data/records.

### Why disable AQE in the baseline?

If the experiment is intended to expose the raw skew behavior, an automatic optimizer could change the execution before we observe the baseline. The baseline should isolate the mechanism under investigation.

### Why test salting?

Salting changes the distribution of a hot key so that work can be spread across multiple partitions. It trades simpler distribution for additional query complexity and potentially more work.

### What is the biggest limitation of the analyzer?

It is a diagnostic signal engine, not a universal causal inference system. A diagnosis should be validated against the execution plan, Spark UI, workload design and controlled comparisons.
