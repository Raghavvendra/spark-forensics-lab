# 4. Event-Log Evidence Guide

## Why the event log matters

The final runtime tells you that something was slow. The event log helps explain **where the time went**.

This repository parses Spark event-log events such as stage submission/completion and task completion, then aggregates task-level metrics into stage summaries.

## Task-level fields used by the lab

| Field | Why it matters |
|---|---|
| task duration | identifies stragglers |
| executor run time | denominator for GC share |
| JVM GC time | memory/GC pressure signal |
| shuffle read bytes | data movement and partition imbalance |
| shuffle read records | distribution signal when bytes look similar |
| shuffle write bytes | upstream data movement |
| memory spill | execution-memory pressure |
| disk spill | memory pressure reaching disk |
| input bytes | stage input volume |
| input records | row-volume signal |

## How the parser treats attempts

The parser keeps the latest successful task attempt for a stage attempt and prefers a completed successful stage attempt over a failed/incomplete attempt when multiple attempts exist.

This matters because a failed or speculative attempt should not silently replace the successful execution used for diagnosis.

## Important interpretation rule

A diagnosis is not proof of causality.

For example:

- high spill can be caused by memory pressure
- memory pressure can be caused by skew
- GC can be a separate bottleneck or a consequence of pressure

The correct engineering response is to compare multiple signals and, when necessary, run a controlled experiment.
