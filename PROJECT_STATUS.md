# Spark Forensics Lab — Documentation Status

This documentation pack was written against the current public repository structure.

## Confirmed in the repository

- package name: `spark-forensics-lab`
- Python requirement: `>=3.10`
- PySpark optional dependency: `>=4.2,<4.3`
- pytest optional dependency: `>=8,<10`
- CLI entry point: `spark-forensics-analyze`
- event-log parser module: `src/spark_forensics/eventlog.py`
- diagnosis module: `src/spark_forensics/diagnose.py`
- models module: `src/spark_forensics/models.py`
- tests for event-log parsing and diagnosis
- current README focus: join skew

## Deliberately not claimed

This pack does not invent benchmark numbers, Spark UI evidence, or successful mitigation results. Those must come from an actual run.

## What to do with this pack

Copy the contents of `docs/`, `evidence/`, `notes/`, and optionally `README_REPLACEMENT.md` into your local clone of the repository.

Then run the existing experiment(s), save the evidence, and replace every `TODO` with measured values.
