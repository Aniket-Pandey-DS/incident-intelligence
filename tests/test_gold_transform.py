from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.gold_transform import build_incident_summary

def test_build_incident_summary(spark: SparkSession):
    data = [
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", "RESOLVED", 60.0),
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", "RESOLVED", 120.0),
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", "OPEN", None),
        (date(2026, 1, 1), "search", "eu-west-1", "SEV2", "OPEN", None),
    ]

    columns = [
        "created_date",
        "service",
        "region",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=columns)

    gold_df = build_incident_summary(silver_df)

    checkout = (
        gold_df
        .filter("service = 'checkout'")
        .first()
    )

    assert checkout["incident_count"] == 3
    assert checkout["resolved_count"] == 2
    assert checkout["open_count"] == 1
    assert checkout["avg_duration_minutes"] == 90.0

    search = (
        gold_df
        .filter("service = 'search'")
        .first()
    )

    assert search["incident_count"] == 1
    assert search["resolved_count"] == 0
    assert search["open_count"] == 1
    assert search["avg_duration_minutes"] is None


def test_summary_keeps_different_groups_separate(spark: SparkSession):
    schema = StructType([
    StructField("created_date", DateType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("incident_duration_minutes", DoubleType(), True),
    ])

    data = [
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV1", "OPEN", None),
        (date(2026, 1, 1), "checkout", "us-west-2", "SEV2", "OPEN", None),
        (date(2026, 1, 2), "checkout", "us-west-2", "SEV1", "OPEN", None),
    ]

    columns = [
        "created_date",
        "service",
        "region",
        "severity",
        "status",
        "incident_duration_minutes",
    ]

    silver_df = spark.createDataFrame(data, schema=schema)

    gold_df = build_incident_summary(silver_df)

    assert gold_df.count() == 3
