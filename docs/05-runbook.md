# 5. Runbook

This document is the practical checklist for running a forensic experiment.

## Before the run

Record:

```text
Date:
Git commit:
Spark version:
Python version:
Machine/runtime:
Input row count:
Input size:
Important Spark configuration:
Experiment name:
Hypothesis:
```

## Execute

1. Generate or prepare the deterministic dataset.
2. Run the baseline workload.
3. Enable Spark event logging.
4. Preserve the event-log output.
5. Run the analyzer against the event log.
6. Record the stage selected for investigation.
7. Capture only the evidence needed to support the conclusion.

## After the run

Save artifacts using a predictable name:

```text
 evidence/
   <experiment>-baseline-event-log-reference.txt
   <experiment>-baseline-stage.png
   <experiment>-comparison.csv
   <experiment>-notes.md
```

Large raw event logs should generally stay outside Git unless there is a specific reason to version them.

## Repeatability

A run is considered reproducible only when another person can reconstruct the workload and configuration from the repository without relying on your memory.

## Failure checklist

If the analyzer reports no useful signal:

1. verify that the event log actually contains successful task metrics
2. verify that the chosen stage contains multiple tasks
3. inspect the Spark UI for the relevant stage
4. confirm the baseline configuration actually disabled competing optimizations when appropriate
5. check whether the workload produced enough skew to make the effect visible
