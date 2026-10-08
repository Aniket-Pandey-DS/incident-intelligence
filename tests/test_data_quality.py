from pathlib import Path

from pyspark.sql import SparkSession

from src.data_quality import apply_quality_rules, load_quality_rules

from src.data_quality import (
    apply_quality_rules,
    load_quality_rules,
    validate_unique_columns,
)

from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = PROJECT_ROOT / "configs" / "incident_quality_rules.yaml"


def create_test_spark():
    return (
        SparkSession.builder
        .appName("IncidentIntelligence-DataQualityTests")
        .master("local[2]")
        .getOrCreate()
    )


def test_quality_rules_detect_invalid_records():
    spark = create_test_spark()

    try:
        data = [
            ("INC-001", "SEV1", 100, 5.0, 50.0, 60.0),
            ("INC-002", "SEV99", 100, 5.0, 50.0, 60.0),
            ("INC-003", "SEV2", -10, 5.0, 50.0, 60.0),
            ("INC-004", "SEV2", 100, 150.0, 50.0, 60.0),
        ]

        columns = [
            "incident_id",
            "severity",
            "affected_users",
            "error_rate",
            "cpu_utilization",
            "memory_utilization",
        ]

        df = spark.createDataFrame(data, columns)

        # Add columns required by the quality rules.
        df = (
            df.withColumn(
                "created_at",
                F.to_timestamp(F.lit("2026-01-01 10:00:00"))
            )
            .withColumn(
                "resolved_at",
                F.to_timestamp(F.lit("2026-01-01 11:00:00"))
            )
            .withColumn("category", F.lit("performance"))
            .withColumn("status", F.lit("RESOLVED"))
            .withColumn("latency_ms", F.lit(100.0))
            .withColumn("request_volume", F.lit(1000))
        )

        rules = [
            rule
            for rule in load_quality_rules(RULES_PATH)
            if rule["check"] != "unique"
        ]
        result = apply_quality_rules(df, rules)

        invalid_records = result.filter(
            "size(quality_issues) > 0"
        )

        assert invalid_records.count() == 3

    finally:
        spark.stop()


def test_unique_column_validation():
    spark = create_test_spark()

    try:
        data = [
            ("INC-001",),
            ("INC-002",),
            ("INC-001",),
            ("INC-003",),
        ]

        df = spark.createDataFrame(
            data,
            ["incident_id"],
        )

        duplicates = validate_unique_columns(
            df,
            "incident_id",
        )

        assert duplicates.count() == 1
        assert duplicates.first()["incident_id"] == "INC-001"
        assert duplicates.first()["count"] == 2

    finally:
        spark.stop()