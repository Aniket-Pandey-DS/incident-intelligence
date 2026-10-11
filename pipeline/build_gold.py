
from pathlib import Path

from pyspark.sql import SparkSession

from src.gold_transform import build_incident_summary
from src.gold_quality import (
    validate_gold_schema,
    validate_incident_counts,
    validate_group_counts,
    validate_unique_grain,
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
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "incidents"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "incident_summary"
RUN_LOG_PATH = (
    PROJECT_ROOT
    / "data"
    / "observability"
    / "build_gold_runs.jsonl"
)


def main() -> None:
    metadata = create_run_metadata("build_gold")
    spark = None

    try:
        spark = (
            SparkSession.builder
            .appName("IncidentIntelligence-BuildGold")
            .master("local[*]")
            .getOrCreate()
        )

        silver_df = read_published_parquet(
            spark,
            str(SILVER_PATH),
        )
        silver_count = silver_df.count()

        gold_df = build_incident_summary(silver_df)
        gold_count = gold_df.count()

        update_run_counts(metadata, silver_count, gold_count)

        # Validate Gold output before publishing.
        validate_gold_schema(gold_df)
        validate_incident_counts(silver_count, gold_df)
        validate_group_counts(gold_df)
        validate_unique_grain(gold_df)

        mark_quality_gate(metadata, True)

        published_path = publish_parquet(
            gold_df,
            spark,
            str(GOLD_PATH),
        )

        published_df = read_published_parquet(
            spark,
            str(GOLD_PATH),
        )
        published_count = published_df.count()

        if published_count != gold_count:
            raise ValueError(
                "Published row count does not match validated Gold count."
            )

        mark_run_success(metadata, str(published_path))

        print(f"Run ID: {metadata['run_id']}")
        print(f"Silver records read: {silver_count}")
        print(f"Gold summary rows written: {gold_count}")
        print(f"Published rows verified: {published_count}")
        print(f"Gold columns: {len(published_df.columns)}")
        print("Gold quality gate: PASSED")
        print(f"Published version path: {published_path}")
        print(f"Current manifest: {GOLD_PATH / 'current.json'}")

        published_df.orderBy(
            "created_date",
            "service",
            "region",
            "severity",
        ).show(10, truncate=False)

    except Exception as error:
        mark_run_failure(metadata, error)
        raise

    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            append_run_metadata_safely(
                metadata,
                str(RUN_LOG_PATH),
            )


if __name__ == "__main__":
    main()
