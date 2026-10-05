from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
EVENT_LOG_DIR = ROOT / "artifacts" / "spark-events"
RESULT_DIR = ROOT / "artifacts" / "skew-result"


def main() -> None:
    # The event-log directory must exist before Spark starts.
    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("spark-forensics-skew")
        .master("local[4]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.resolve().as_uri())
        # Spark 4 defaults to a rolling, zstd-compressed log directory. Turn both
        # off so the lab produces one plain JSON-lines file you can grep.
        .config("spark.eventLog.rolling.enabled", "false")
        .config("spark.eventLog.compress", "false")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    try:
        fact_rows = 1_000_000
        hot_rows = 800_000
        dim_rows = 100_000

        fact = (
            spark.range(fact_rows)
            .withColumn(
                "key",
                F.when(F.col("id") < hot_rows, F.lit(0))
                .otherwise((F.col("id") % dim_rows).cast("long")),
            )
            .withColumn("value", (F.col("id") % 17).cast("long"))
            .repartition(32, "key")
        )

        dim = (
            spark.range(dim_rows)
            .select(F.col("id").alias("key"), F.col("id").alias("dim_value"))
            .repartition(32, "key")
        )

        joined = fact.join(dim.hint("merge"), on="key", how="inner")

        print("\n--- Physical plan ---")
        joined.explain("formatted")

        joined.write.mode("overwrite").parquet(str(RESULT_DIR))
        print(f"\nexpected joined rows: {fact_rows}")
        print(f"event log: {EVENT_LOG_DIR}")
        print(f"result:    {RESULT_DIR}")
        print(f"python:    {sys.version.split()[0]}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
