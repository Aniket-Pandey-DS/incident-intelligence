
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from src.data_quality import load_quality_rules
from src.incremental_ingestion import process_incident_batch
from src.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INCOMING_PATH = PROJECT_ROOT / "data" / "incoming" / "incidents"
SCHEMA_PATH = PROJECT_ROOT / "configs" / "incident_schema.yaml"
QUALITY_RULES_PATH = (
    PROJECT_ROOT / "configs" / "incident_quality_rules.yaml"
)

# Keep the incremental experiment isolated from the existing pipeline.
BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "incremental_incidents"
)
QUARANTINE_PATH = (
    PROJECT_ROOT / "data" / "quarantine" / "incremental_incidents"
)
CHECKPOINT_PATH = (
    PROJECT_ROOT / "data" / "state" / "incident_ingestion.json"
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process one incremental incident CSV batch."
    )
    parser.add_argument(
        "batch_id",
        help="Batch ID without the .csv extension, e.g. batch_0001",
    )
    args = parser.parse_args()

    batch_file = INCOMING_PATH / f"{args.batch_id}.csv"

    if not batch_file.is_file():
        parser.error(f"Batch file not found: {batch_file}")

    spark = (
        SparkSession.builder
        .appName("IncidentIntelligence-IncrementalIngestion")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        schema = load_schema(SCHEMA_PATH)
        rules = load_quality_rules(QUALITY_RULES_PATH)

        incoming_df = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(str(batch_file))
        )

        result = process_incident_batch(
            spark=spark,
            incoming_df=incoming_df,
            batch_id=args.batch_id,
            rules=rules,
            bronze_path=BRONZE_PATH,
            quarantine_path=QUARANTINE_PATH,
            checkpoint_path=CHECKPOINT_PATH,
        )

        print("=== Incremental Ingestion Result ===")
        for key, value in result.items():
            print(f"{key}: {value}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
