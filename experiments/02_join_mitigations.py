from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_forensics.diagnose import diagnose_stage
from spark_forensics.eventlog import parse_event_log


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts" / "experiment-01"

MODES = ("baseline", "aqe", "salted")
SALT_BUCKETS = 16
FACT_ROWS = 1_000_000
HOT_ROWS = 800_000
DIM_ROWS = 100_000
SHUFFLE_PARTITIONS = 32


def build_spark(mode: str) -> SparkSession:
    event_dir = ARTIFACTS / mode / "spark-events"
    if event_dir.exists():
        shutil.rmtree(event_dir)
    event_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(f"spark-forensics-join-{mode}")
        .master("local[4]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.compress", "false")
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )

    if mode == "baseline" or mode == "salted":
        builder = builder.config("spark.sql.adaptive.enabled", "false")
    elif mode == "aqe":
        builder = (
            builder.config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2.0")
            # The dataset is intentionally small enough that Spark's default
            # 256 MB threshold would hide the effect. This keeps the test local.
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1m")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        )
    else:
        raise ValueError(f"unknown mode: {mode}")

    return builder.getOrCreate()


def build_data(spark: SparkSession, mode: str) -> tuple[DataFrame, DataFrame]:
    fact = (
        spark.range(FACT_ROWS)
        .withColumn(
            "key",
            F.when(F.col("id") < HOT_ROWS, F.lit(0))
            .otherwise((F.col("id") % DIM_ROWS).cast("long")),
        )
        .withColumn("value", (F.col("id") % 17).cast("long"))
    )

    dim = (
        spark.range(DIM_ROWS)
        .select(F.col("id").alias("key"), F.col("id").alias("dim_value"))
    )

    if mode == "salted":
        fact = (
            fact.withColumn(
                "salt",
                F.when(F.col("key") == 0, F.col("id") % SALT_BUCKETS).otherwise(F.lit(0)),
            )
            .repartition(SHUFFLE_PARTITIONS, "key", "salt")
        )

        salt_values = spark.range(SALT_BUCKETS).select(F.col("id").cast("long").alias("salt"))
        hot_dim = dim.filter(F.col("key") == 0).crossJoin(salt_values)
        normal_dim = dim.filter(F.col("key") != 0).withColumn("salt", F.lit(0).cast("long"))
        dim = hot_dim.unionByName(normal_dim).repartition(SHUFFLE_PARTITIONS, "key", "salt")
        return fact, dim

    fact = fact.repartition(SHUFFLE_PARTITIONS, "key")
    dim = dim.repartition(SHUFFLE_PARTITIONS, "key")
    return fact, dim


def run_join(spark: SparkSession, mode: str) -> Path:
    result_dir = ARTIFACTS / mode / "result"
    if result_dir.exists():
        shutil.rmtree(result_dir)

    fact, dim = build_data(spark, mode)

    if mode == "salted":
        joined = fact.join(dim.hint("merge"), on=["key", "salt"], how="inner").select(
            "id", "key", "value", "dim_value"
        )
    else:
        joined = fact.join(dim.hint("merge"), on="key", how="inner").select(
            "id", "key", "value", "dim_value"
        )

    print(f"\n--- {mode} physical plan ---")
    joined.explain("formatted")

    joined.write.mode("overwrite").parquet(str(result_dir))
    print(f"result: {result_dir}")
    return result_dir


def find_event_log(mode: str) -> Path:
    event_dir = ARTIFACTS / mode / "spark-events"
    files = sorted(p for p in event_dir.iterdir() if p.is_file())
    if len(files) != 1:
        raise RuntimeError(f"expected one event-log file in {event_dir}, found {len(files)}")
    return files[0]


def summarize_run(mode: str) -> dict[str, object]:
    log_path = find_event_log(mode)
    stages = parse_event_log(log_path)
    shuffle_stages = [
        stage for stage in stages
        if len(stage.tasks) >= 2 and sum(task.shuffle_read_bytes for task in stage.tasks) > 0
    ]
    if not shuffle_stages:
        diagnoses = []
        chosen = None
    else:
        chosen = max(
            shuffle_stages,
            key=lambda stage: sum(task.shuffle_read_bytes for task in stage.tasks),
        )
        diagnoses = diagnose_stage(chosen)

    evidence = {}
    for diagnosis in diagnoses:
        evidence[diagnosis.code] = diagnosis.evidence

    summary = {
        "mode": mode,
        "event_log": str(log_path.relative_to(ROOT)),
        "stage_id": None if chosen is None else chosen.stage_id,
        "stage_status": None if chosen is None else chosen.status,
        "task_count": 0 if chosen is None else len(chosen.tasks),
        "diagnoses": [diagnosis.code for diagnosis in diagnoses],
        "evidence": evidence,
    }
    return summary


def compare_results(spark: SparkSession) -> None:
    paths = {mode: ARTIFACTS / mode / "result" for mode in MODES}
    baseline = spark.read.parquet(str(paths["baseline"])).select("id", "key", "value", "dim_value")

    for mode in ("aqe", "salted"):
        candidate = spark.read.parquet(str(paths[mode])).select("id", "key", "value", "dim_value")
        left_only = baseline.exceptAll(candidate).limit(1).count()
        right_only = candidate.exceptAll(baseline).limit(1).count()
        if left_only or right_only:
            raise RuntimeError(f"result mismatch: baseline vs {mode} (left_only={left_only}, right_only={right_only})")
        print(f"result equality: baseline == {mode}")


def run_mode(mode: str) -> dict[str, object]:
    spark = build_spark(mode)
    started = time.perf_counter()
    try:
        run_join(spark, mode)
    finally:
        elapsed = time.perf_counter() - started
        spark.stop()
    summary = summarize_run(mode)
    summary["write_wall_time_seconds"] = round(elapsed, 3)
    print(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=MODES + ("all",), default="all")
    args = parser.parse_args()

    ARTIFACTS.mkdir(parents=True, exist_ok=True)

    if args.mode == "all":
        summaries = []
        for mode in MODES:
            summaries.append(run_mode(mode))
        spark = SparkSession.builder.master("local[2]").appName("spark-forensics-verify").getOrCreate()
        try:
            compare_results(spark)
        finally:
            spark.stop()
        (ARTIFACTS / "comparison-summary.json").write_text(
            json.dumps(summaries, indent=2) + "\n", encoding="utf-8"
        )
        print(f"comparison summary: {ARTIFACTS / 'comparison-summary.json'}")
    else:
        run_mode(args.mode)


if __name__ == "__main__":
    main()
