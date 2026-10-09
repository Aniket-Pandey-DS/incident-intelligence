from datetime import datetime
from pyspark.sql import SparkSession
from src.silver_transform import transform_incidents

def test_unresolved_incident_has_null_duration(spark):
    schema = """
        incident_id STRING,
        created_at TIMESTAMP,
        resolved_at TIMESTAMP,
        service STRING,
        region STRING,
        severity STRING,
        status STRING
    """

    data = [
        (
            "INC-TEST-001",
            datetime(2026, 1, 1, 10, 0),
            None,
            " payments ",
            " us-east-1 ",
            "SEV1",
            "OPEN",
        )
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transform_incidents(bronze_df)

    row = silver_df.first()

    assert row["incident_duration_minutes"] is None
    assert row["is_resolved"] is False
    assert row["severity_priority"] == 1
    assert row["created_date"] == datetime(2026, 1, 1).date()
    assert row["service"] == "payments"
    assert row["region"] == "us-east-1"



def test_resolved_incident_duration(spark):
    schema = """
        incident_id STRING,
        created_at TIMESTAMP,
        resolved_at TIMESTAMP,
        service STRING,
        region STRING,
        severity STRING,
        status STRING
    """

    data = [
        (
            "INC-TEST-002",
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 11, 30),
            "payments",
            "us-east-1",
            "SEV2",
            "RESOLVED",
        )
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transform_incidents(bronze_df)

    row = silver_df.first()

    assert row["incident_duration_minutes"] == 90.0
    assert row["is_resolved"] is True
    assert row["severity_priority"] == 2



def test_inconsistent_resolution_status_is_flagged(spark):
    schema = """
        incident_id STRING,
        created_at TIMESTAMP,
        resolved_at TIMESTAMP,
        service STRING,
        region STRING,
        severity STRING,
        status STRING
    """

    data = [
        (
            "INC-TEST-003",
            datetime(2026, 1, 1, 10, 0),
            None,
            "payments",
            "us-east-1",
            "SEV1",
            "RESOLVED",
        ),
        (
            "INC-TEST-004",
            datetime(2026, 1, 1, 10, 0),
            datetime(2026, 1, 1, 11, 0),
            "payments",
            "us-east-1",
            "SEV1",
            "OPEN",
        ),
    ]

    bronze_df = spark.createDataFrame(data, schema=schema)
    silver_df = transform_incidents(bronze_df)

    results = {
        row["incident_id"]: row["resolution_status_consistent"]
        for row in silver_df.collect()
    }

    assert results["INC-TEST-003"] is False
    assert results["INC-TEST-004"] is False
