from pathlib import Path
from pyspark.sql import SparkSession
from src.gold_transform import build_resolution_performance
from src.gold_quality import (
    validate_resolution_metrics,
    validate_resolution_performance_schema,
    validate_resolution_counts,
    validate_resolution_unique_grain,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "incidents"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "resolution_performance"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("IncidentIntelligence-ResolutionPerformance")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # Read the Silver dataset
        silver_df = spark.read.parquet(str(SILVER_PATH))

        # Transform Silver into Gold Resolution Performance
        gold_df = build_resolution_performance(silver_df)

        # Validate Gold output before writing
        validate_resolution_performance_schema(gold_df)
        validate_resolution_counts(silver_df, gold_df)
        validate_resolution_metrics(gold_df)
        validate_resolution_unique_grain(gold_df)

        gold_count = gold_df.count()

        # Write the validated Gold dataset
        gold_df.write.mode("overwrite").parquet(str(GOLD_PATH))

        print(f"Silver records read: {silver_df.count()}")
        print(f"Gold resolution performance rows written: {gold_count}")
        print(f"Gold columns: {len(gold_df.columns)}")
        print("Gold Resolution Performance quality gate: PASSED")
        print(f"Gold output path: {GOLD_PATH}")

        gold_df.orderBy(
            "resolution_date",
            "service",
            "severity",
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
