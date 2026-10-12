
import pytest
from pyspark.sql import SparkSession
import re
import json
from src.incremental_ingestion import (
    merge_incident_batch,
    validate_incoming_batch,
    load_checkpoint,
    process_incident_batch,
    read_published_parquet,
)

def test_merge_inserts_new_incidents(spark: SparkSession):
    existing = spark.createDataFrame(
        [(1, "checkout")],
        ["incident_id", "service"],
    )
    incoming = spark.createDataFrame(
        [(2, "payments")],
        ["incident_id", "service"],
    )

    result = merge_incident_batch(existing, incoming)

    assert {
        tuple(row)
        for row in result.collect()
    } == {
        (1, "checkout"),
        (2, "payments"),
    }


def test_merge_replaces_existing_incident(spark: SparkSession):
    existing = spark.createDataFrame(
        [(1, "checkout")],
        ["incident_id", "service"],
    )
    incoming = spark.createDataFrame(
        [(1, "payments")],
        ["incident_id", "service"],
    )

    result = merge_incident_batch(existing, incoming)

    assert [
        tuple(row)
        for row in result.collect()
    ] == [(1, "payments")]


def test_merge_rejects_duplicate_incoming_ids(
    spark: SparkSession,
):
    existing = spark.createDataFrame(
        [(1, "checkout")],
        ["incident_id", "service"],
    )
    incoming = spark.createDataFrame(
        [(2, "payments"), (2, "search")],
        ["incident_id", "service"],
    )

    with pytest.raises(
        ValueError,
        match="duplicate incident_id",
    ):
        merge_incident_batch(existing, incoming)


def test_merge_rejects_schema_mismatch(spark: SparkSession):
    existing = spark.createDataFrame(
        [(1, "checkout")],
        ["incident_id", "service"],
    )
    incoming = spark.createDataFrame(
        [(2, "payments", "us-east-1")],
        ["incident_id", "service", "region"],
    )

    with pytest.raises(
        ValueError,
        match="identical columns",
    ):
        merge_incident_batch(existing, incoming)


def test_validation_quarantines_invalid_records(spark, tmp_path):
    from src.data_quality import load_quality_rules
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]
    rules = [
    {
        "id": "severity_valid",
        "column": "severity",
        "check": "allowed_values",
        "values": ["SEV1", "SEV2", "SEV3", "SEV4"],
    },
    {
        "id": "incident_id_unique",
        "column": "incident_id",
        "check": "unique",
    },
    ]

    incoming = spark.createDataFrame(
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV99", 100),
        ],
        ["incident_id", "severity", "affected_users"],
    )

    valid, invalid = validate_incoming_batch(incoming, rules)

    assert valid.count() == 1
    assert invalid.count() == 1
    assert invalid.first()["incident_id"] == "INC-10002"
    assert "severity_valid" in invalid.first()["quality_issues"]


def test_validation_quarantines_all_duplicate_ids(spark, tmp_path):
    from src.data_quality import load_quality_rules
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[1]
    rules = [
    {
        "id": "incident_id_unique",
        "column": "incident_id",
        "check": "unique",
    },
    ]

    incoming = spark.createDataFrame(
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10001", "SEV2", 200),
        ],
        ["incident_id", "severity", "affected_users"],
    )

    valid, invalid = validate_incoming_batch(incoming, rules)

    assert valid.count() == 0
    assert invalid.count() == 2
    assert all(
        "incident_id_unique" in row["quality_issues"]
        for row in invalid.collect()
    )


def batch_rules():
    return [
        {
            "id": "incident_id_not_null",
            "column": "incident_id",
            "check": "not_null",
        },
        {
            "id": "severity_valid",
            "column": "severity",
            "check": "allowed_values",
            "values": ["SEV1", "SEV2", "SEV3", "SEV4"],
        },
        {
            "id": "incident_id_unique",
            "column": "incident_id",
            "check": "unique",
        },
    ]


def sample_batch(spark, rows):
    return spark.createDataFrame(
        rows,
        ["incident_id", "severity", "affected_users"],
    )


def test_process_first_batch_publishes_and_checkpoints(spark, tmp_path):
    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV2", 200),
        ],
    )

    result = process_incident_batch(
        spark=spark,
        incoming_df=incoming,
        batch_id="batch_0001",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    assert result["status"] == "processed"
    assert result["valid_records"] == 2
    assert result["quarantined_records"] == 0
    assert result["published_records"] == 2
    assert load_checkpoint(checkpoint) == ["batch_0001"]
    assert (bronze / "current.json").is_file()


def test_process_repeated_batch_skips(spark, tmp_path):
    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [("INC-10001", "SEV1", 100)],
    )

    kwargs = {
        "spark": spark,
        "incoming_df": incoming,
        "batch_id": "batch_0001",
        "rules": batch_rules(),
        "bronze_path": bronze,
        "quarantine_path": quarantine,
        "checkpoint_path": checkpoint,
    }

    first = process_incident_batch(**kwargs)
    second = process_incident_batch(**kwargs)

    assert first["status"] == "processed"
    assert second["status"] == "skipped"
    assert second["reason"] == "already_processed"


def test_process_batch_quarantines_invalid_records(spark, tmp_path):
    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV99", 200),
        ],
    )

    result = process_incident_batch(
        spark=spark,
        incoming_df=incoming,
        batch_id="batch_0001",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    assert result["valid_records"] == 1
    assert result["quarantined_records"] == 1
    assert result["published_records"] == 1
    assert (quarantine / "batch_0001").is_dir()


def test_process_rejects_unsafe_batch_id(spark, tmp_path):
    incoming = sample_batch(
        spark,
        [("INC-10001", "SEV1", 100)],
    )

    with pytest.raises(ValueError, match="unsafe characters"):
        process_incident_batch(
            spark=spark,
            incoming_df=incoming,
            batch_id="../outside",
            rules=batch_rules(),
            bronze_path=tmp_path / "bronze",
            quarantine_path=tmp_path / "quarantine",
            checkpoint_path=tmp_path / "state.json",
        )


def test_publication_failure_does_not_advance_checkpoint(
    spark, tmp_path, monkeypatch
):
    import src.incremental_ingestion as ingestion

    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [("INC-10001", "SEV1", 100)],
    )

    def fail_publication(*args, **kwargs):
        raise RuntimeError("simulated publication failure")

    monkeypatch.setattr(
        ingestion,
        "publish_parquet",
        fail_publication,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated publication failure",
    ):
        ingestion.process_incident_batch(
            spark=spark,
            incoming_df=incoming,
            batch_id="batch_0001",
            rules=batch_rules(),
            bronze_path=tmp_path / "bronze",
            quarantine_path=tmp_path / "quarantine",
            checkpoint_path=checkpoint,
        )

    assert load_checkpoint(checkpoint) == []




def test_checkpoint_failure_allows_safe_batch_replay(
    spark, tmp_path, monkeypatch
):
    import src.incremental_ingestion as ingestion

    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV2", 200),
        ],
    )

    original_save_checkpoint = ingestion.save_checkpoint

    def fail_checkpoint(*args, **kwargs):
        raise OSError("simulated checkpoint failure")

    monkeypatch.setattr(
        ingestion,
        "save_checkpoint",
        fail_checkpoint,
    )

    with pytest.raises(
        OSError,
        match="simulated checkpoint failure",
    ):
        ingestion.process_incident_batch(
            spark=spark,
            incoming_df=incoming,
            batch_id="batch_0001",
            rules=batch_rules(),
            bronze_path=bronze,
            quarantine_path=quarantine,
            checkpoint_path=checkpoint,
        )

    # Bronze was published, but the batch is not checkpointed.
    assert (bronze / "current.json").is_file()
    assert load_checkpoint(checkpoint) == []

    # Restore checkpoint persistence and replay the same batch.
    monkeypatch.setattr(
        ingestion,
        "save_checkpoint",
        original_save_checkpoint,
    )

    result = ingestion.process_incident_batch(
        spark=spark,
        incoming_df=incoming,
        batch_id="batch_0001",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    published = ingestion.read_published_parquet(
        spark,
        str(bronze),
    )

    assert result["status"] == "processed"
    assert published.count() == 2
    assert published.select("incident_id").distinct().count() == 2
    assert load_checkpoint(checkpoint) == ["batch_0001"]



def test_second_batch_inserts_and_updates_incidents(spark, tmp_path):
    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    first_batch = sample_batch(
        spark,
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV2", 200),
        ],
    )

    second_batch = sample_batch(
        spark,
        [
            ("INC-10002", "SEV3", 250),  # Updated incident
            ("INC-10003", "SEV1", 300),  # New incident
        ],
    )

    first_result = process_incident_batch(
        spark=spark,
        incoming_df=first_batch,
        batch_id="batch_0001",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    second_result = process_incident_batch(
        spark=spark,
        incoming_df=second_batch,
        batch_id="batch_0002",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    published = read_published_parquet(spark, str(bronze))
    rows = {
        row["incident_id"]: row.asDict()
        for row in published.collect()
    }

    assert first_result["published_records"] == 2
    assert second_result["published_records"] == 3
    assert published.count() == 3
    assert rows["INC-10001"]["severity"] == "SEV1"
    assert rows["INC-10002"]["severity"] == "SEV3"
    assert rows["INC-10002"]["affected_users"] == 250
    assert rows["INC-10003"]["severity"] == "SEV1"
    assert load_checkpoint(checkpoint) == [
        "batch_0001",
        "batch_0002",
    ]



def test_batch_with_invalid_records_can_retry_after_publication_failure(
    spark, tmp_path, monkeypatch
):
    import src.incremental_ingestion as ingestion

    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"
    checkpoint = tmp_path / "state" / "incident_ingestion.json"

    incoming = sample_batch(
        spark,
        [
            ("INC-10001", "SEV1", 100),
            ("INC-10002", "SEV99", 200),
        ],
    )

    original_publish = ingestion.publish_parquet

    def fail_publication(*args, **kwargs):
        raise RuntimeError("simulated publication failure")

    monkeypatch.setattr(
        ingestion, "publish_parquet", fail_publication
    )

    # First attempt: quarantine succeeds, Bronze publication fails.
    with pytest.raises(
        RuntimeError, match="simulated publication failure"
    ):
        ingestion.process_incident_batch(
            spark=spark,
            incoming_df=incoming,
            batch_id="batch_0001",
            rules=batch_rules(),
            bronze_path=bronze,
            quarantine_path=quarantine,
            checkpoint_path=checkpoint,
        )

    assert (quarantine / "batch_0001").is_dir()
    assert load_checkpoint(checkpoint) == []

    quarantined = spark.read.parquet(
        str(quarantine / "batch_0001")
    )

    assert [
        row["incident_id"]
        for row in quarantined.select("incident_id").collect()
    ] == ["INC-10002"]

    # Second attempt: publication is restored and the same batch is retried.
    monkeypatch.setattr(
        ingestion, "publish_parquet", original_publish
    )

    result = ingestion.process_incident_batch(
        spark=spark,
        incoming_df=incoming,
        batch_id="batch_0001",
        rules=batch_rules(),
        bronze_path=bronze,
        quarantine_path=quarantine,
        checkpoint_path=checkpoint,
    )

    assert result["status"] == "processed"
    assert result["valid_records"] == 1
    assert result["quarantined_records"] == 1
    assert result["published_records"] == 1
    assert load_checkpoint(checkpoint) == ["batch_0001"]

