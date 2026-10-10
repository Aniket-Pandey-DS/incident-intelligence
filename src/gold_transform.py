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



def build_resolution_performance(silver_df: DataFrame) -> DataFrame:
    """Aggregate resolved incidents into a daily performance summary."""

    return (
        silver_df
        .filter(
            (F.col("status") == "RESOLVED")
            & F.col("resolved_at").isNotNull()
        )
        .groupBy(
            F.to_date("resolved_at").alias("resolution_date"),
            "service",
            "severity",
        )
        .agg(
            F.count("*").alias("resolved_incident_count"),
            F.count("incident_duration_minutes").alias(
                "duration_observation_count"
            ),
            F.avg("incident_duration_minutes").alias(
                "avg_resolution_minutes"
            ),
            F.median(
                "incident_duration_minutes"
            ).alias("median_resolution_minutes"),
            F.max("incident_duration_minutes").alias(
                "max_resolution_minutes"
            ),
        )
    )
