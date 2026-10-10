from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_incident_summary(silver_df: DataFrame) -> DataFrame:
    """Aggregate Silver incidents into a daily business summary."""

    return (
        silver_df
        .groupBy(
            "created_date",
            "service",
            "region",
            "severity",
        )
        .agg(
            F.count("*").alias("incident_count"),
            F.sum(
                F.when(F.col("status") == "RESOLVED", 1).otherwise(0)
            ).alias("resolved_count"),
            F.sum(
                F.when(F.col("status") == "OPEN", 1).otherwise(0)
            ).alias("open_count"),
            F.avg("incident_duration_minutes").alias(
                "avg_duration_minutes"
            ),
        )
    )
