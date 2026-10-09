from pathlib import Path
from pyspark.sql import SparkSession
from src.silver_transform import transform_incidents

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

        silver_df.write.mode("overwrite").parquet(str(SILVER_PATH))

        print(f"Bronze records read: {bronze_df.count()}")
        print(f"Silver records written: {silver_df.count()}")
        print(f"Silver columns: {len(silver_df.columns)}")
        print(f"Silver output path: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
