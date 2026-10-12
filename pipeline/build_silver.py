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
from src.run_observability import (
    create_run_metadata,
    update_run_counts,
    mark_quality_gate,
    mark_run_success,
    mark_run_failure,
    append_run_metadata_safely,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "incidents"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "incidents"
RUN_LOG_PATH = PROJECT_ROOT / "data" / "observability" / "build_silver_runs.jsonl" 


def main() -> None:
    metadata = create_run_metadata("build_silver")
    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("IncidentIntelligence-BuildSilver")
            .master("local[*]")
            .getOrCreate()
        )

        bronze_df = read_published_parquet(spark, str(BRONZE_PATH))
        bronze_count = bronze_df.count()

        silver_df = transform_incidents(bronze_df)
        silver_count = silver_df.count()

        update_run_counts(metadata, bronze_count, silver_count)

        # Validate Silver DataFrame
        validate_silver_schema(silver_df)
        validate_record_count(bronze_count, silver_count)
        validate_incident_durations(silver_df)
        validate_derived_fields(silver_df)
        validate_lifecycle_consistency(silver_df)

        mark_quality_gate(metadata, True)

        published_path = publish_parquet(
            silver_df,
            spark,
            str(SILVER_PATH),
        )

        published_df = read_published_parquet(
            spark,
            str(SILVER_PATH),
        )
        published_count = published_df.count()

        if published_count != silver_count:
            raise ValueError(
                "Published row count does not match validated Silver count."
            )

        mark_run_success(metadata, str(published_path))

        print(f"Run ID: {metadata['run_id']}")
        print(f"Bronze records read: {bronze_count}")
        print(f"Silver records: {silver_count}")
        print(f"Published rows verified: {published_count}")
        print(f"Silver columns: {len(published_df.columns)}")
        print("Silver quality gate: PASSED")
        print(f"Published version path: {published_path}")
        print(f"Current manifest: {SILVER_PATH / 'current.json'}")

    except Exception as error:
        mark_run_failure(metadata, error)
        raise

    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            append_run_metadata_safely(metadata, str(RUN_LOG_PATH))


if __name__ == "__main__":
    main()
