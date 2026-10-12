
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.data_quality import (
    apply_quality_rules,
    validate_unique_columns,
)
import json
import os
import re
import tempfile
from pathlib import Path
from src.data_publishing import (
    publish_parquet,
    read_published_parquet,
)


def merge_incident_batch(
    existing_df: DataFrame,
    incoming_df: DataFrame,
) -> DataFrame:
    """Merge a batch into the existing snapshot by incident_id.

    Existing records are retained unless the incoming batch contains
    the same incident_id. Incoming records take precedence.
    """

    duplicate_ids = (
        incoming_df
        .groupBy("incident_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicate_ids.limit(1).count() > 0:
        raise ValueError(
            "Incoming batch contains duplicate incident_id values."
        )

    existing_columns = existing_df.columns
    incoming_columns = incoming_df.columns

    if set(existing_columns) != set(incoming_columns):
        raise ValueError(
            "Existing and incoming datasets must have identical columns."
        )

    incoming_aligned = incoming_df.select(*existing_columns)

    existing_only = existing_df.join(
        incoming_aligned.select("incident_id"),
        on="incident_id",
        how="left_anti",
    )

    return existing_only.unionByName(incoming_aligned)



def validate_incoming_batch(
    incoming_df: DataFrame,
    rules: list[dict],
) -> tuple[DataFrame, DataFrame]:
    """Split a batch into valid and quarantined records."""

    row_level_rules = [
        rule for rule in rules
        if rule["check"] != "unique"
    ]

    quality_df = apply_quality_rules(
        incoming_df,
        row_level_rules,
    )

    duplicate_ids = (
        validate_unique_columns(
            incoming_df,
            column="incident_id",
        )
        .select("incident_id")
        .withColumn(
            "duplicate_issue",
            F.lit("incident_id_unique"),
        )
    )

    quality_df = (
        quality_df
        .join(
            duplicate_ids,
            on="incident_id",
            how="left",
        )
        .withColumn(
            "quality_issues",
            F.when(
                F.col("duplicate_issue").isNotNull(),
                F.array_union(
                    F.col("quality_issues"),
                    F.array(F.col("duplicate_issue")),
                ),
            ).otherwise(F.col("quality_issues")),
        )
        .drop("duplicate_issue")
    )

    valid_records = quality_df.filter(
        F.size("quality_issues") == 0
    ).drop("quality_issues")

    invalid_records = quality_df.filter(
        F.size("quality_issues") > 0
    )

    return valid_records, invalid_records



def load_checkpoint(checkpoint_path: str | Path) -> list[str]:
    """Load processed batch IDs, returning an empty list if absent."""
    checkpoint_path = Path(checkpoint_path)

    if not checkpoint_path.exists():
        return []

    with checkpoint_path.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    if not isinstance(checkpoint, dict):
        raise ValueError("Checkpoint must contain a JSON object.")

    processed_batches = checkpoint.get("processed_batches")

    if not isinstance(processed_batches, list):
        raise ValueError(
            "Checkpoint must contain a 'processed_batches' list."
        )

    if not all(
        isinstance(batch_id, str) and batch_id
        for batch_id in processed_batches
    ):
        raise ValueError(
            "Every processed batch ID must be a non-empty string."
        )

    return processed_batches


def save_checkpoint(
    checkpoint_path: str | Path,
    processed_batches: list[str],
) -> None:
    """Atomically replace the checkpoint with the updated batch list."""
    checkpoint_path = Path(checkpoint_path)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    if not all(
        isinstance(batch_id, str) and batch_id
        for batch_id in processed_batches
    ):
        raise ValueError(
            "Every processed batch ID must be a non-empty string."
        )

    checkpoint = {
        "processed_batches": list(dict.fromkeys(processed_batches))
    }

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=checkpoint_path.parent,
            prefix=f".{checkpoint_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_path = Path(file.name)
            json.dump(checkpoint, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_path, checkpoint_path)
        temp_path = None

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)



def process_incident_batch(
    spark,
    incoming_df: DataFrame,
    batch_id: str,
    rules: list[dict],
    bronze_path: str | Path,
    quarantine_path: str | Path,
    checkpoint_path: str | Path,
) -> dict:
    """Validate, merge, publish, and checkpoint one incident batch."""

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id must be a non-empty string.")

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", batch_id):
        raise ValueError("batch_id contains unsafe characters.")

    processed_batches = load_checkpoint(checkpoint_path)

    if batch_id in processed_batches:
        return {
            "batch_id": batch_id,
            "status": "skipped",
            "reason": "already_processed",
        }

    valid_records, invalid_records = validate_incoming_batch(
        incoming_df,
        rules,
    )

    valid_count = valid_records.count()
    invalid_count = invalid_records.count()

    bronze_path = Path(bronze_path)
    quarantine_path = Path(quarantine_path)

    # Read the current published snapshot, if one exists.
    manifest_path = bronze_path / "current.json"

    if manifest_path.is_file():
        existing_df = read_published_parquet(
            spark,
            str(bronze_path),
        )
        merged_df = merge_incident_batch(
            existing_df,
            valid_records,
        )
    else:
        merged_df = valid_records

    # Write quarantine to a batch-specific location so one batch
    # cannot overwrite another batch's quarantined records.
    batch_quarantine_path = quarantine_path / batch_id

    if invalid_count > 0:
        (
            invalid_records.write
            .mode("errorifexists")
            .parquet(str(batch_quarantine_path))
        )

    # Publish Bronze before marking the batch as processed.
    published_path = publish_parquet(
        merged_df,
        spark,
        str(bronze_path),
    )

    # Only checkpoint after successful publication.
    save_checkpoint(
        checkpoint_path,
        processed_batches + [batch_id],
    )

    return {
        "batch_id": batch_id,
        "status": "processed",
        "valid_records": valid_count,
        "quarantined_records": invalid_count,
        "published_records": merged_df.count(),
        "published_path": str(published_path),
    }
