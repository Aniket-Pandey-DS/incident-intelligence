from pathlib import Path
from pyspark.sql import SparkSession
from src.gold_transform import build_incident_summary
from src.gold_quality import (
    validate_gold_schema,
    validate_incident_counts,
    validate_group_counts,
    validate_unique_grain,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "incidents"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "incident_summary"

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("IncidentIntelligence-BuildGold")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        silver_df = spark.read.parquet(str(SILVER_PATH))
        silver_count = silver_df.count()

        gold_df = build_incident_summary(silver_df)

        # Validate Gold output before writing
        validate_gold_schema(gold_df)
        validate_incident_counts(silver_count, gold_df)
        validate_group_counts(gold_df)
        validate_unique_grain(gold_df)

        gold_count = gold_df.count()
        
        gold_df.write.mode("overwrite").parquet(str(GOLD_PATH))

        print(f"Silver records read: {silver_count}")
        print(f"Gold summary rows written: {gold_count}")
        print(f"Gold columns: {len(gold_df.columns)}")
        print("Gold quality gate: PASSED")
        print(f"Gold output path: {GOLD_PATH}")

        gold_df.orderBy(
            "created_date",
            "service",
            "region",
            "severity",
        ).show(10, truncate=False)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
