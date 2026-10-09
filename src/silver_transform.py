from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_incidents(bronze_df: DataFrame) -> DataFrame:
    silver_df = (
        bronze_df
        .withColumn("service", F.trim(F.col("service")))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn(
            "incident_duration_minutes",
            (
                F.col("resolved_at").cast("long") - F.col("created_at").cast("long")
            ) / 60.0
        )
        .withColumn(
            "severity_priority",
            F.when(F.col("severity") == "SEV1", 1)
            .when(F.col("severity") == "SEV2", 2)
            .when(F.col("severity") == "SEV3", 3)
            .when(F.col("severity") == "SEV4", 4)
            .otherwise(None),
        )
        .withColumn(
            "is_resolved", F.col("status") == "RESOLVED",
        )
        .withColumn(
            "created_date", F.to_date(F.col("created_at")),
        )
        .withColumn(
            "resolution_status_consistent",
            (
                (F.col("status") == "RESOLVED") & F.col("resolved_at").isNotNull()
            )
            | (
                (F.col("status") == "OPEN") & F.col("resolved_at").isNull()
            ),
        )
    )
    return silver_df