from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def validate_gold_schema(gold_df: DataFrame) -> None:
    required_columns = {
        "created_date",
        "service",
        "region",
        "severity",
        "incident_count",
        "resolved_count",
        "open_count",
        "avg_duration_minutes",
    }

    missing = required_columns - set(gold_df.columns)

    if missing:
        raise ValueError(
            f"Gold schema is missing columns: {sorted(missing)}"
        )

def validate_incident_counts(
    silver_count: int,
    gold_df: DataFrame,
) -> None:
    gold_count = gold_df.agg(
        F.sum("incident_count").alias("total")
    ).first()["total"]

    if gold_count != silver_count:
        raise ValueError(
            f"Incident count mismatch: Silver={silver_count}, "
            f"Gold total={gold_count}"
        )

def validate_group_counts(gold_df: DataFrame) -> None:
    invalid = gold_df.filter(
        (F.col("incident_count") <= 0)
        | (F.col("resolved_count") < 0)
        | (F.col("open_count") < 0)
        | (
            F.col("resolved_count") + F.col("open_count")
            != F.col("incident_count")
        )
    )

    if invalid.limit(1).count() > 0:
        raise ValueError(
            "Gold contains invalid incident counts or "
            "resolved/open counts that do not reconcile"
        )

def validate_unique_grain(gold_df: DataFrame) -> None:
    grain = ["created_date", "service", "region", "severity"]

    duplicates = (
        gold_df
        .groupBy(*grain)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Gold contains duplicate rows for the defined grain"
        )

def validate_resolution_performance_schema(
    gold_df: DataFrame,
) -> None:
    required_columns = {
        "resolution_date",
        "service",
        "severity",
        "resolved_incident_count",
        "duration_observation_count",
        "avg_resolution_minutes",
        "median_resolution_minutes",
        "max_resolution_minutes",
    }

    missing = required_columns - set(gold_df.columns)

    if missing:
        raise ValueError(
            f"Resolution Performance schema is missing columns: "
            f"{sorted(missing)}"
        )

def validate_resolution_counts(
    silver_df: DataFrame,
    gold_df: DataFrame,
) -> None:
    expected_count = silver_df.filter(
        (F.col("status") == "RESOLVED")
        & F.col("resolved_at").isNotNull()
    ).count()

    actual_count = gold_df.agg(
        F.sum("resolved_incident_count").alias("total")
    ).first()["total"]

    if actual_count != expected_count:
        raise ValueError(
            f"Resolution count mismatch: "
            f"Eligible Silver={expected_count}, "
            f"Gold total={actual_count}"
        )

def validate_resolution_metrics(gold_df: DataFrame) -> None:
    invalid = gold_df.filter(
        # Counts must be valid
        (F.col("resolved_incident_count") <= 0)
        | (F.col("duration_observation_count") < 0)
        | (
            F.col("duration_observation_count")
            > F.col("resolved_incident_count")
        )
        # Duration metrics must be nonnegative when present
        | (F.col("avg_resolution_minutes") < 0)
        | (F.col("median_resolution_minutes") < 0)
        | (F.col("max_resolution_minutes") < 0)
        # If there are no duration observations, metrics must be NULL
        | (
            (F.col("duration_observation_count") == 0)
            & (
                F.col("avg_resolution_minutes").isNotNull()
                | F.col("median_resolution_minutes").isNotNull()
                | F.col("max_resolution_minutes").isNotNull()
            )
        )
        # If observations exist, all duration metrics must be present
        | (
            (F.col("duration_observation_count") > 0)
            & (
                F.col("avg_resolution_minutes").isNull()
                | F.col("median_resolution_minutes").isNull()
                | F.col("max_resolution_minutes").isNull()
            )
        )
    )

    if invalid.limit(1).count() > 0:
        raise ValueError(
            "Resolution Performance contains invalid counts "
            "or inconsistent duration metrics"
        )
    
def validate_resolution_unique_grain(gold_df: DataFrame) -> None:
    grain = [
        "resolution_date",
        "service",
        "severity",
    ]

    duplicates = (
        gold_df
        .groupBy(*grain)
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Resolution Performance contains duplicate rows "
            "for the defined grain"
        )
