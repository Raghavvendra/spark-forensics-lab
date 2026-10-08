# Spark Forensics Lab

I am using this repository to understand why Spark jobs become slow.

The method is simple: create a workload with a known problem, capture the Spark event log, inspect task-level metrics, and use the evidence to form a diagnosis.

## Current experiment: join skew

The first experiment creates an intentionally uneven join workload. The objective is to see how skew appears in Spark execution rather than judging the job only by total runtime.

The forensic analysis looks at:

- task duration
- shuffle read
- shuffle records
- spill
- JVM GC time

The analyzer then applies simple signals to identify likely bottlenecks.

## Comparison

The same logical workload can be compared under different execution strategies, such as Adaptive Query Execution and salting. The important rule is to keep the data and query semantics equivalent so that the comparison is meaningful.

## Repository structure

```text
src/                     event-log parser and diagnosis logic
experiments/             reproducible Spark workloads
tests/                   parser and diagnosis tests
docs/                    reasoning, experiment notes, and runbooks
evidence/                screenshots and measured outputs
```

## How to read this project

Start with `docs/01-project-overview.md`, then `docs/02-forensic-method.md`, and finally the experiment note for the workload you want to investigate.

This repository records what was actually observed. No benchmark result is claimed until the workload has been executed and the evidence has been saved.
