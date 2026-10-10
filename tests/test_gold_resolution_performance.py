from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.gold_transform import build_resolution_performance

RESOLUTION_SCHEMA = StructType([
    StructField("resolved_at", TimestampType(), True),
    StructField("service", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("incident_duration_minutes", DoubleType(), True),
])

def test_build_resolution_performance(spark: SparkSession):
    data = [
        (datetime(2026, 1, 1, 10, 0), "checkout", "SEV1", "RESOLVED", 30.0),
        (datetime(2026, 1, 1, 11, 0), "checkout", "SEV1", "RESOLVED", 90.0),
        (datetime(2026, 1, 1, 12, 0), "checkout", "SEV1", "OPEN", None),
        (datetime(2026, 1, 1, 13, 0), "search", "SEV2", "RESOLVED", 30.0),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=columns)

    gold_df = build_resolution_performance(silver_df)

    checkout = (
        gold_df
        .filter("service = 'checkout'")
        .first()
    )

    assert checkout["resolved_incident_count"] == 2
    assert checkout["duration_observation_count"] == 2
    assert checkout["avg_resolution_minutes"] == 60.0
    assert checkout["median_resolution_minutes"] == 60.0
    assert checkout["max_resolution_minutes"] == 90.0

    search = (
        gold_df
        .filter("service = 'search'")
        .first()
    )

    assert search["resolved_incident_count"] == 1
    assert search["duration_observation_count"] == 1
    assert search["avg_resolution_minutes"] == 30.0
    assert search["median_resolution_minutes"] == 30.0
    assert search["max_resolution_minutes"] == 30.0

def test_resolution_performance_keeps_dates_separate(spark: SparkSession):
    from datetime import datetime

    data = [
        (datetime(2026, 1, 1, 10), "checkout", "SEV1", "RESOLVED", 30.0),
        (datetime(2026, 1, 2, 10), "checkout", "SEV1", "RESOLVED", 60.0),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=columns)
    gold_df = build_resolution_performance(silver_df)

    assert gold_df.count() == 2

    dates = {
        row["resolution_date"]
        for row in gold_df.select("resolution_date").collect()
    }

    assert dates == {date(2026, 1, 1), date(2026, 1, 2)}

def test_resolution_performance_keeps_services_and_severities_separate(
    spark: SparkSession,
):
    from datetime import datetime

    data = [
        (datetime(2026, 1, 1, 10), "checkout", "SEV1", "RESOLVED", 30.0),
        (datetime(2026, 1, 1, 11), "search", "SEV1", "RESOLVED", 60.0),
        (datetime(2026, 1, 1, 12), "checkout", "SEV2", "RESOLVED", 90.0),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=columns)
    gold_df = build_resolution_performance(silver_df)

    assert gold_df.count() == 3

def test_resolution_performance_handles_null_durations(
    spark: SparkSession,
):
    from datetime import datetime

    data = [
        (datetime(2026, 1, 1, 10), "checkout", "SEV1", "RESOLVED", 30.0),
        (datetime(2026, 1, 1, 11), "checkout", "SEV1", "RESOLVED", None),
        (datetime(2026, 1, 1, 12), "checkout", "SEV1", "RESOLVED", 90.0),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=columns)
    gold_df = build_resolution_performance(silver_df)

    row = gold_df.first()

    assert row["resolved_incident_count"] == 3
    assert row["duration_observation_count"] == 2
    assert row["avg_resolution_minutes"] == 60.0
    assert row["median_resolution_minutes"] == 60.0
    assert row["max_resolution_minutes"] == 90.0

def test_resolution_performance_handles_all_null_durations(
    spark: SparkSession,
):
    from datetime import datetime

    data = [
        (datetime(2026, 1, 1, 10), "checkout", "SEV1", "RESOLVED", None),
        (datetime(2026, 1, 1, 11), "checkout", "SEV1", "RESOLVED", None),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=RESOLUTION_SCHEMA)
    gold_df = build_resolution_performance(silver_df)

    row = gold_df.first()

    assert row["resolved_incident_count"] == 2
    assert row["duration_observation_count"] == 0
    assert row["avg_resolution_minutes"] is None
    assert row["median_resolution_minutes"] is None
    assert row["max_resolution_minutes"] is None

def test_resolution_performance_excludes_open_incidents(
    spark: SparkSession,
):
    from datetime import datetime

    data = [
        (datetime(2026, 1, 1, 10), "checkout", "SEV1", "OPEN", None),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=RESOLUTION_SCHEMA)
    gold_df = build_resolution_performance(silver_df)

    assert gold_df.count() == 0

def test_resolution_performance_excludes_null_resolution_timestamp(
    spark: SparkSession,
):
    from datetime import datetime

    data = [
        (None, "checkout", "SEV1", "RESOLVED", 30.0),
    ]

    columns = [
        "resolved_at",
        "service",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=RESOLUTION_SCHEMA)
    gold_df = build_resolution_performance(silver_df)

    assert gold_df.count() == 0

def test_resolution_performance_handles_empty_input(
    spark: SparkSession,
):
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType([
        StructField("resolved_at", TimestampType(), True),
        StructField("service", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("status", StringType(), True),
        StructField("incident_duration_minutes", DoubleType(), True),
    ])

    silver_df = spark.createDataFrame([], schema=schema)
    gold_df = build_resolution_performance(silver_df)

    assert gold_df.count() == 0

    expected_columns = {
        "resolution_date",
        "service",
        "severity",
        "resolved_incident_count",
        "duration_observation_count",
        "avg_resolution_minutes",
        "median_resolution_minutes",
        "max_resolution_minutes",
    }

    assert set(gold_df.columns) == expected_columns

