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
