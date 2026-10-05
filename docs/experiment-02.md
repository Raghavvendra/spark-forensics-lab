# Experiment 02: one cause, two fixes

Experiment 01 established that the workload can create a real skewed join.

Experiment 02 asks a narrower question:

> Can two different mitigations reduce the skew without changing the result?

The controls are intentionally small.

| Run | AQE | Salting | Purpose |
|---|---|---|---|
| baseline | off | no | reference case |
| aqe | skew join on | no | let Spark split the skewed partition |
| salted | off | yes | change the partitioning key for the hot key |

The data is deterministic. The fact table has 1,000,000 rows, 800,000 of which use the hot key `0`. The dimension has 100,000 unique keys. Broadcast is disabled in all three cases.

For the AQE run, the skew threshold is deliberately lower than Spark's normal production-scale default because this is a small local experiment. That setting is part of the experiment; it should not be carried forward blindly.

For the salted run, the hot key is split into 16 deterministic buckets. The dimension row for the hot key is replicated across those buckets. The output drops the salt column, so the logical result remains the same.

The script writes separate event logs and result directories under `artifacts/experiment-01/` and checks exact multiset equality between the baseline result and the two mitigated results.

The script has been syntax-tested here, but the actual Spark comparison must be run on a machine with PySpark and Java. The repository intentionally contains no fabricated performance numbers.
