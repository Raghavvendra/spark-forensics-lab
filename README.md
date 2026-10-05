Spark Forensics Lab

I am using this repository to understand why Spark jobs become slow.

The idea is simple: create a workload with a known problem, collect the Spark event log, and investigate what actually happened instead of guessing from the final runtime.

Current experiment

The first experiment looks at join skew.

The workload is intentionally uneven, so some tasks receive much more data than others.

The event log is then used to examine:

task duration

shuffle read

records processed

spill

JVM GC time

The analyzer uses those measurements to identify the likely bottleneck.

Next experiment

The same workload is being compared with:

Adaptive Query Execution

salting

The data and query remain the same. The goal is to see what changes in the execution and whether the result remains identical.

Notes

This repository is being built experiment by experiment.

