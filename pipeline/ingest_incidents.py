from pathlib import Path

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

from src.schema_loader import load_schema
from src.data_quality import (
    apply_quality_rules,
    load_quality_rules,
    validate_unique_columns,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "incidents" / "incidents.csv"
SCHEMA_PATH = PROJECT_ROOT / "configs" / "incident_schema.yaml"
QUALITY_RULES_PATH = PROJECT_ROOT / "configs" / "incident_quality_rules.yaml"
BRONZE_DATA_PATH = PROJECT_ROOT / "data" / "bronze" / "incidents"
QUARANTINE_DATA_PATH = PROJECT_ROOT / "data" / "quarantine" / "incidents"


def create_spark_session() -> SparkSession:
    """Create the Spark session for the incident ingestion pipeline."""
    return (
        SparkSession.builder
        .appName("IncidentIntelligence-IngestIncidents")
        .master("local[*]")
        .getOrCreate()
    )


def ingest_incidents() -> None:
    """Read raw incident data using the canonical data contract."""
    spark = create_spark_session()

    try:
        schema = load_schema(SCHEMA_PATH)

        incidents_df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(str(RAW_DATA_PATH))
        )

        rules = load_quality_rules(QUALITY_RULES_PATH)

        row_level_rules = [
            rule
            for rule in rules
            if rule["check"] != "unique"
        ]

        duplicate_incident_ids = validate_unique_columns(
            incidents_df,
            column="incident_id",
        )

        duplicate_ids = duplicate_incident_ids.select(
            "incident_id"
        ).withColumn(
            "duplicate_issue",
            F.lit("incident_id_unique"),
        )

        quality_df = apply_quality_rules(
            incidents_df,
            row_level_rules
        )

        duplicate_ids = duplicate_incident_ids.select(
            "incident_id"
        ).withColumn(
            "duplicate_issue",
            F.lit("incident_id_unique"),
        )

        quality_df = (
            quality_df.join(
                duplicate_ids,
                on="incident_id",
                how="left"
            )
            .withColumn(
                "quality_issues",
                F.when(
                    F.col("duplicate_issue").isNotNull(),
                    F.array_union(
                        F.col("quality_issues"),
                        F.array(F.col("duplicate_issue"))
                    )
                ).otherwise(F.col("quality_issues"))
            )
            .drop("duplicate_issue")
        )

        valid_records = quality_df.filter("size(quality_issues) = 0")
        invalid_records = quality_df.filter("size(quality_issues) > 0")

        print("=== Quality Summary ===")
        print(f"Total records: {quality_df.count()}")
        print(f"Valid records: {valid_records.count()}")
        print(f"Invalid records: {invalid_records.count()}")

        print("=== Invalid Records ===")
        invalid_records.select(
            "incident_id",
            "quality_issues",
        ).show(truncate=False)

        print("=== Incident DataFrame Schema ===")
        incidents_df.printSchema()

        print("=== Incident Record Count ===")
        print(incidents_df.count())

        print("=== Sample Records ===")
        incidents_df.show(5, truncate=False)

        BRONZE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

        (
            valid_records.drop("quality_issues")
            .write
            .mode("overwrite")
            .parquet(str(BRONZE_DATA_PATH))
        )

        print(f"Bronze data written to: {BRONZE_DATA_PATH}")

        QUARANTINE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

        (
            invalid_records.write
            .mode("overwrite")
            .parquet(str(QUARANTINE_DATA_PATH))
        )

        print(f"Quarantine data written to: {QUARANTINE_DATA_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    ingest_incidents()