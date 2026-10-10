import pytest
from pyspark.sql.types import *
from datetime import date

from src.silver_quality import (
    EXPECTED_SILVER_COLUMNS,
    validate_silver_schema,
    validate_record_count,
    validate_incident_durations,
    validate_derived_fields,
    validate_lifecycle_consistency,
)

def test_validate_silver_schema_passes_when_all_columns_exist(spark):
    schema = StructType(
        [
            StructField(column, StringType(), True)
            for column in EXPECTED_SILVER_COLUMNS
        ]
    )
    silver_df = spark.createDataFrame([], schema)

    validate_silver_schema(silver_df)


def test_validate_silver_schema_fails_when_column_is_missing(spark):
    columns = sorted(EXPECTED_SILVER_COLUMNS - {"severity_priority"})

    schema = StructType(
        [
            StructField(column, StringType(), True)
            for column in columns
        ]
    )
    silver_df = spark.createDataFrame([], schema)

    with pytest.raises(ValueError, match="severity_priority"):
        validate_silver_schema(silver_df)

def test_validate_record_count_passes_when_counts_match():
    validate_record_count(bronze_count=92, silver_count=92)

def test_validate_record_count_fails_when_counts_differ():
    with pytest.raises(ValueError, match="Bronze count: 92, Silver count: 91"):
        validate_record_count(bronze_count=92, silver_count=91)


def test_validate_incident_durations_passes_for_valid_data(spark):
    schema = StructType(
        [
            StructField("is_resolved", BooleanType(), True),
            StructField("incident_duration_minutes", DoubleType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            (True, 45.0),
            (True, 90.0),
            (False, None),
        ],
        schema,
    )

    validate_incident_durations(silver_df)


def test_validate_incident_durations_fails_for_invalid_data(spark):
    schema = StructType(
        [
            StructField("is_resolved", BooleanType(), True),
            StructField("incident_duration_minutes", DoubleType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            (True, None),
            (True, -10.0),
            (False, 45.0),
        ],
        schema,
    )

    with pytest.raises(ValueError, match="Found 3 incidents"):
        validate_incident_durations(silver_df)



def test_validate_derived_fields_passes_for_correct_values(spark):
    schema = StructType(
        [
            StructField("severity", StringType(), True),
            StructField("severity_priority", IntegerType(), True),
            StructField("created_at", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("is_resolved", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            ("SEV1", 1, "2026-10-01 10:00:00",
             date(2026, 10, 1), "RESOLVED", True),
            ("SEV2", 2, "2026-10-02 11:30:00",
             date(2026, 10, 2), "OPEN", False),
        ],
        schema,
    )

    validate_derived_fields(silver_df)


def test_validate_derived_fields_fails_for_incorrect_values(spark):
    schema = StructType(
        [
            StructField("severity", StringType(), True),
            StructField("severity_priority", IntegerType(), True),
            StructField("created_at", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("is_resolved", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            # Incorrect priority
            ("SEV1", 4, "2026-10-01 10:00:00",
             date(2026, 10, 1), "RESOLVED", True),
            # Incorrect created date
            ("SEV2", 2, "2026-10-02 11:30:00",
             date(2026, 10, 3), "OPEN", False),
            # Incorrect resolution flag
            ("SEV3", 3, "2026-10-04 09:00:00",
             date(2026, 10, 4), "RESOLVED", False),
        ],
        schema,
    )

    with pytest.raises(ValueError, match="Found 3 incidents"):
        validate_derived_fields(silver_df)


def test_validate_lifecycle_consistency_passes_for_valid_data(spark):
    schema = StructType(
        [
            StructField("status", StringType(), True),
            StructField("resolved_at", StringType(), True),
            StructField("resolution_status_consistent", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            ("RESOLVED", "2026-10-01 10:00:00", True),
            ("OPEN", None, True),
        ],
        schema,
    )

    validate_lifecycle_consistency(silver_df)

def test_validate_lifecycle_consistency_fails_for_invalid_data(spark):
    schema = StructType(
        [
            StructField("status", StringType(), True),
            StructField("resolved_at", StringType(), True),
            StructField("resolution_status_consistent", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            # RESOLVED but resolved_at is null
            ("RESOLVED", None, False),
            # OPEN but resolved_at is not null
            ("OPEN", "2026-10-02 11:30:00", False),
            # resolution_status_consistent is null
            ("RESOLVED", "2026-10-03 09:00:00", None),
        ],
        schema,
    )

    with pytest.raises(ValueError, match="Found 3 inconsistent incidents"):
        validate_lifecycle_consistency(silver_df)


def test_validate_derived_fields_fails_when_priority_is_null(spark):
    schema = StructType(
        [
            StructField("severity", StringType(), True),
            StructField("severity_priority", IntegerType(), True),
            StructField("created_at", StringType(), True),
            StructField("created_date", DateType(), True),
            StructField("status", StringType(), True),
            StructField("is_resolved", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            (
                "SEV1",
                None,
                "2026-10-01 10:00:00",
                date(2026, 10, 1),
                "RESOLVED",
                True,
            )
        ],
        schema,
    )

    with pytest.raises(ValueError, match="Found 1 incidents"):
        validate_derived_fields(silver_df)



def test_validate_incident_durations_fails_when_is_resolved_is_null(spark):
    schema = StructType(
        [
            StructField("is_resolved", BooleanType(), True),
            StructField("incident_duration_minutes", DoubleType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [(None, None)],
        schema,
    )

    with pytest.raises(ValueError, match="Found 1 incidents"):
        validate_incident_durations(silver_df)


def test_validate_lifecycle_consistency_fails_when_status_is_null(spark):
    schema = StructType(
        [
            StructField("status", StringType(), True),
            StructField("resolved_at", StringType(), True),
            StructField("resolution_status_consistent", BooleanType(), True),
        ]
    )

    silver_df = spark.createDataFrame(
        [
            (None, None, True),
        ],
        schema,
    )

    with pytest.raises(ValueError, match="Found 1 inconsistent incidents"):
        validate_lifecycle_consistency(silver_df)