# First real run: skew experiment

Environment: Spark 4.2.0, Java 21, Python 3.12, `local[4]`, 1M fact rows (800k on one
hot key), 100k dimension rows, 32 shuffle partitions, broadcast and AQE off.

## Plan check (before trusting anything)
`SortMergeJoin`, with `Exchange hashpartitioning(key, 32)` on both sides. The join is
the intended one. Output verified: 1,000,000 rows written, as expected.

## What the analyzer reported for the join stage (stage 2, 32 tasks)
| Signal | Run A | Run B | Run C |
|---|---|---|---|
| Largest task's share of shuffle bytes | 0.66 | 0.66 | 0.66 |
| Largest task's share of shuffle records | 0.736 | 0.736 | 0.736 |
| vs. uniform share (1/32) | 21.1x | 21.1x | 21.1x |
| Max task time / median | 4.66 | 5.25 | 6.80 |
| Spill | 0 | 0 | 0 |

The hot task read about 6.7 MB and 809k records; typical tasks read about 0.1 MB and
about 10k records.

## What this does and does not show
- Byte and record shares are identical across runs (deterministic); the timing ratio ranged from 4.7 to 6.8.
  Trust the shuffle share more than the timing ratio.
- The hot task holds about 74% of rows but 66% of bytes: identical keys compress
  well in shuffle files, so bytes understate the row skew. `Total Records Read` is in
  the log and is a better skew signal. The analyzer now uses it (23.6x uniform here vs 21.1x for bytes).
- Absolute times are tiny (about 2 s), so max/median is only just above the 3.0
  threshold. Scale the data up before judging the threshold.
- One machine, one query, three runs. This is evidence the analyzer works on a real log,
  not a performance claim.

## Two things the real log taught us that the tests could not
1. Spark 4.x writes a rolling, zstd-compressed log *directory* by default; the parser
   crashed on it. The parser now reads directories and zstd; the workload also turns
   both off so the log is a plain greppable file.
2. Real shuffle fields are `Remote Bytes Read` and `Local Bytes Read` (confirmed).
