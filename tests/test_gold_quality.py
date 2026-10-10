from datetime import date
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.gold_quality import (
    validate_gold_schema,
    validate_incident_counts,
    validate_group_counts,
    validate_unique_grain,
)

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
