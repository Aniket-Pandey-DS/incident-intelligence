from pathlib import Path
from pyspark.sql import SparkSession
from src.silver_transform import transform_incidents
from src.silver_quality import (
    validate_silver_schema,
    validate_record_count,
    validate_incident_durations,
    validate_derived_fields,
    validate_lifecycle_consistency,
)
from src.data_publishing import (
    publish_parquet,
    read_published_parquet,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "incidents"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "incidents"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("IncidentIntelligence-BuildSilver")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        bronze_df = spark.read.parquet(str(BRONZE_PATH))

        silver_df = transform_incidents(bronze_df)

        # Validate Silver DataFrame
        validate_silver_schema(silver_df)
        validate_record_count(bronze_df.count(), silver_df.count())
        validate_incident_durations(silver_df)
        validate_derived_fields(silver_df)
        validate_lifecycle_consistency(silver_df)

        published_path = publish_parquet(
            silver_df,
            spark,
            str(SILVER_PATH),
        )

        published_df = read_published_parquet(
            spark,
            str(SILVER_PATH),
        )

        print(f"Bronze records read: {bronze_df.count()}")
        print(f"Silver records: {silver_df.count()}")
        print(f"Published rows verified: {published_df.count()}")
        print(f"Silver columns: {len(published_df.columns)}")
        print("Silver quality gate: PASSED")
        print(f"Published version path: {published_path}")
        print(f"Current manifest: {SILVER_PATH / 'current.json'}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
