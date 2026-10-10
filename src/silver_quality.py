from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EXPECTED_SILVER_COLUMNS = {
    # Original incident columns
    "incident_id",
    "created_at",
    "resolved_at",
    "service",
    "region",
    "severity",
    "category",
    "status",
    "description",
    "affected_users",
    "error_rate",
    "latency_ms",
    "request_volume",
    "cpu_utilization",
    "memory_utilization",
    "root_cause",
    "resolution",
    "resolver_team",
    "source_system",
    "environment",
    "created_by",
    "ingested_at",
    # Derived Silver columns
    "incident_duration_minutes",
    "severity_priority",
    "is_resolved",
    "created_date",
    "resolution_status_consistent",
}


def validate_silver_schema(silver_df: DataFrame) -> None:
    """Raise an error if required Silver columns are missing."""
    actual_columns = set(silver_df.columns)
    missing_columns = EXPECTED_SILVER_COLUMNS - actual_columns

    if missing_columns:
        raise ValueError(
            "Silver schema validation failed. "
            f"Missing columns: {sorted(missing_columns)}"
        )


def validate_record_count(
    bronze_count: int,
    silver_count: int,
) -> None:
    """Raise an error if Silver changes the Bronze record count."""
    if bronze_count != silver_count:
        raise ValueError(
            "Silver record-count validation failed. "
            f"Bronze count: {bronze_count}, "
            f"Silver count: {silver_count}"
        )


def validate_incident_durations(silver_df: DataFrame) -> None:
    """Validate incident durations for resolved and unresolved incidents."""

    invalid_durations = silver_df.filter(
        # The resolution flag itself must be present.
        F.col("is_resolved").isNull()
        |
        # Resolved incidents need a non-negative duration.
        (
            (F.col("is_resolved") == True)
            & (
                F.col("incident_duration_minutes").isNull()
                | (F.col("incident_duration_minutes") < 0)
            )
        )
        |
        # Unresolved incidents must have no duration.
        (
            (F.col("is_resolved") == False)
            & F.col("incident_duration_minutes").isNotNull()
        )
    )

    invalid_count = invalid_durations.count()

    if invalid_count > 0:
        raise ValueError(
            "Silver duration validation failed. "
            f"Found {invalid_count} incidents with invalid durations."
        )


def validate_derived_fields(silver_df: DataFrame) -> None:
    """Validate derived Silver columns against their source fields."""

    expected_priority = (
        F.when(F.col("severity") == "SEV1", 1)
        .when(F.col("severity") == "SEV2", 2)
        .when(F.col("severity") == "SEV3", 3)
        .when(F.col("severity") == "SEV4", 4)
    )

    invalid_priority = (
        F.col("severity").isNull()
        | (~F.col("severity").isin("SEV1", "SEV2", "SEV3", "SEV4"))
        | F.col("severity_priority").isNull()
        | (F.col("severity_priority") != expected_priority)
    )

    expected_created_date = F.to_date(F.col("created_at"))

    invalid_created_date = (
        F.col("created_at").isNull()
        | expected_created_date.isNull()
        | F.col("created_date").isNull()
        | (expected_created_date != F.col("created_date"))
    )

    expected_is_resolved = F.col("status") == "RESOLVED"

    invalid_resolution_flag = (
        F.col("status").isNull()
        | (~F.col("status").isin("RESOLVED", "OPEN"))
        | F.col("is_resolved").isNull()
        | (F.col("is_resolved") != expected_is_resolved)
    )

    invalid_rows = silver_df.filter(
        invalid_priority
        | invalid_created_date
        | invalid_resolution_flag
    )

    invalid_count = invalid_rows.count()

    if invalid_count > 0:
        raise ValueError(
            "Silver derived-field validation failed. "
            f"Found {invalid_count} incidents with incorrect derived values."
        )




def validate_lifecycle_consistency(silver_df: DataFrame) -> None:
    """Ensure incident status agrees with its resolution timestamp."""

    invalid_rows = silver_df.filter(
        # Status must be recognized.
        F.col("status").isNull()
        | (~F.col("status").isin("RESOLVED", "OPEN"))
        |
        # RESOLVED incidents must have a resolution timestamp.
        (
            (F.col("status") == "RESOLVED")
            & F.col("resolved_at").isNull()
        )
        |
        # OPEN incidents must not have a resolution timestamp.
        (
            (F.col("status") == "OPEN")
            & F.col("resolved_at").isNotNull()
        )
        |
        # The derived consistency flag must be explicitly True.
        (
            F.col("resolution_status_consistent").isNull()
            | (F.col("resolution_status_consistent") == False)
        )
    )

    invalid_count = invalid_rows.count()

    if invalid_count > 0:
        raise ValueError(
            "Silver lifecycle validation failed. "
            f"Found {invalid_count} inconsistent incidents."
        )

