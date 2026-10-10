from datetime import date
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.gold_quality import (
    validate_gold_schema,
    validate_incident_counts,
    validate_group_counts,
    validate_unique_grain,
    validate_resolution_performance_schema,
    validate_resolution_counts,
    validate_resolution_metrics,
    validate_resolution_unique_grain,
)

RESOLUTION_PERFORMANCE_SCHEMA = StructType([
    StructField("resolution_date", DateType(), True),
    StructField("service", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("resolved_incident_count", IntegerType(), True),
    StructField("duration_observation_count", IntegerType(), True),
    StructField("avg_resolution_minutes", DoubleType(), True),
    StructField("median_resolution_minutes", DoubleType(), True),
    StructField("max_resolution_minutes", DoubleType(), True),
])

@pytest.fixture
def valid_gold_df(spark: SparkSession):
    data = [
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", 3, 2, 1, 90.0),
        (date(2026, 1, 1), "search", "eu-west-1", "SEV2", 1, 0, 1, None),
    ]

    columns = [
        "created_date",
        "service",
        "region",
        "severity",
        "incident_count",
        "resolved_count",
        "open_count",
        "avg_duration_minutes",
    ]

    return spark.createDataFrame(data, schema=columns)


def test_validate_gold_schema_passes(valid_gold_df):
    validate_gold_schema(valid_gold_df)


def test_validate_gold_schema_fails_when_column_missing(spark):
    df = spark.createDataFrame(
        [(date(2026, 1, 1), "checkout")],
        ["created_date", "service"],
    )

    with pytest.raises(ValueError, match="missing columns"):
        validate_gold_schema(df)


def test_validate_incident_counts_passes(valid_gold_df):
    validate_incident_counts(4, valid_gold_df)


def test_validate_incident_counts_fails(valid_gold_df):
    with pytest.raises(ValueError, match="Incident count mismatch"):
        validate_incident_counts(5, valid_gold_df)


def test_validate_group_counts_passes(valid_gold_df):
    validate_group_counts(valid_gold_df)


def test_validate_group_counts_fails_when_counts_do_not_reconcile(spark):
    data = [
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", 3, 1, 1, 90.0),
    ]

    columns = [
        "created_date",
        "service",
        "region",
        "severity",
        "incident_count",
        "resolved_count",
        "open_count",
        "avg_duration_minutes",
    ]

    df = spark.createDataFrame(data, schema=columns)

    with pytest.raises(ValueError, match="do not reconcile"):
        validate_group_counts(df)


def test_validate_group_counts_fails_on_nonpositive_total(spark):
    schema = StructType([
    StructField("created_date", DateType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("incident_count", IntegerType(), True),
    StructField("resolved_count", IntegerType(), True),
    StructField("open_count", IntegerType(), True),
    StructField("avg_duration_minutes", DoubleType(), True),
    ])

    data = [
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", 0, 0, 0, None),
    ]

    columns = [
        "created_date",
        "service",
        "region",
        "severity",
        "incident_count",
        "resolved_count",
        "open_count",
        "avg_duration_minutes",
    ]

    df = spark.createDataFrame(data, schema=schema)

    with pytest.raises(ValueError, match="invalid incident counts"):
        validate_group_counts(df)


def test_validate_unique_grain_passes(valid_gold_df):
    validate_unique_grain(valid_gold_df)


def test_validate_unique_grain_fails_on_duplicate(spark):
    row = (
        date(2026, 1, 1),
        "checkout",
        "us-west-2",
        "SEV1",
        1,
        1,
        0,
        60.0,
    )

    df = spark.createDataFrame(
        [row, row],
        [
            "created_date",
            "service",
            "region",
            "severity",
            "incident_count",
            "resolved_count",
            "open_count",
            "avg_duration_minutes",
        ],
    )

    with pytest.raises(ValueError, match="duplicate rows"):
        validate_unique_grain(df)


def test_validate_resolution_performance_schema_passes(spark: SparkSession):
    columns = [
        "resolution_date",
        "service",
        "severity",
        "resolved_incident_count",
        "duration_observation_count",
        "avg_resolution_minutes",
        "median_resolution_minutes",
        "max_resolution_minutes",
    ]

    gold_df = spark.createDataFrame([], schema=RESOLUTION_PERFORMANCE_SCHEMA)

    validate_resolution_performance_schema(gold_df)


def test_validate_resolution_performance_schema_missing_column(
    spark: SparkSession,
):
    missing_column_schema = StructType([
    field
    for field in RESOLUTION_PERFORMANCE_SCHEMA.fields
    if field.name != "max_resolution_minutes"])

    gold_df = spark.createDataFrame([], schema=missing_column_schema)

    with pytest.raises(
        ValueError,
        match="Resolution Performance schema is missing columns",
    ):
        validate_resolution_performance_schema(gold_df)

def test_validate_resolution_counts_passes(spark: SparkSession):
    from datetime import datetime

    silver_data = [
        (datetime(2026, 10, 1, 10, 0), "RESOLVED"),
        (datetime(2026, 10, 1, 11, 0), "RESOLVED"),
        (None, "RESOLVED"),
        (None, "OPEN"),
    ]

    silver_df = spark.createDataFrame(
        silver_data,
        ["resolved_at", "status"],
    )

    gold_df = spark.createDataFrame(
        [
            (2,),
        ],
        ["resolved_incident_count"],
    )

    validate_resolution_counts(silver_df, gold_df)


def test_validate_resolution_counts_fails(spark: SparkSession):
    from datetime import datetime

    silver_data = [
        (datetime(2026, 10, 1, 10, 0), "RESOLVED"),
        (datetime(2026, 10, 1, 11, 0), "RESOLVED"),
    ]

    silver_df = spark.createDataFrame(
        silver_data,
        ["resolved_at", "status"],
    )

    gold_df = spark.createDataFrame(
        [
            (1,),
        ],
        ["resolved_incident_count"],
    )

    with pytest.raises(
        ValueError,
        match="Resolution count mismatch",
    ):
        validate_resolution_counts(silver_df, gold_df)

def test_validate_resolution_metrics_passes(spark: SparkSession):
    gold_df = spark.createDataFrame(
        [
            (2, 2, 60.0, 45.0, 90.0),
            (1, 0, None, None, None),
            (1, 1, 0.0, 0.0, 0.0),
        ],
        [
            "resolved_incident_count",
            "duration_observation_count",
            "avg_resolution_minutes",
            "median_resolution_minutes",
            "max_resolution_minutes",
        ],
    )

    validate_resolution_metrics(gold_df)


def test_validate_resolution_metrics_fails_on_invalid_metrics(
    spark: SparkSession,
):
    gold_df = spark.createDataFrame(
        [
            (1, 2, 60.0, 45.0, 90.0),
        ],
        [
            "resolved_incident_count",
            "duration_observation_count",
            "avg_resolution_minutes",
            "median_resolution_minutes",
            "max_resolution_minutes",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Resolution Performance contains invalid counts",
    ):
        validate_resolution_metrics(gold_df)

def test_validate_resolution_unique_grain_passes(
    spark: SparkSession,
):
    gold_df = spark.createDataFrame(
        [
            ("2026-10-01", "checkout", "SEV1"),
            ("2026-10-01", "payments", "SEV2"),
        ],
        [
            "resolution_date",
            "service",
            "severity",
        ],
    )

    validate_resolution_unique_grain(gold_df)


def test_validate_resolution_unique_grain_fails_on_duplicates(
    spark: SparkSession,
):
    gold_df = spark.createDataFrame(
        [
            ("2026-10-01", "checkout", "SEV1"),
            ("2026-10-01", "checkout", "SEV1"),
        ],
        [
            "resolution_date",
            "service",
            "severity",
        ],
    )

    with pytest.raises(
        ValueError,
        match="Resolution Performance contains duplicate rows",
    ):
        validate_resolution_unique_grain(gold_df)
