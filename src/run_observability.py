from datetime import datetime, timezone
from uuid import uuid4
import json
from pathlib import Path
import sys

def utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def create_run_metadata(pipeline_name: str) -> dict:
    """Create the initial metadata record for a pipeline run."""
    return {
        "run_id": str(uuid4()),
        "pipeline_name": pipeline_name,
        "status": "RUNNING",
        "started_at": utc_now(),
        "finished_at": None,
        "input_records": None,
        "output_records": None,
        "quality_gate_passed": None,
        "published_version_path": None,
        "error_message": None,
    }

def update_run_counts(
    metadata: dict,
    input_records: int,
    output_records: int,
) -> None:
    """Record input and output row counts."""
    if input_records < 0 or output_records < 0:
        raise ValueError("Record counts cannot be negative.")

    metadata["input_records"] = input_records
    metadata["output_records"] = output_records


def mark_quality_gate(
    metadata: dict,
    passed: bool,
) -> None:
    """Record whether the quality gate passed."""
    metadata["quality_gate_passed"] = passed


def mark_run_success(
    metadata: dict,
    published_version_path: str,
) -> None:
    """Mark the run successful after publication is verified."""
    metadata["status"] = "SUCCESS"
    metadata["published_version_path"] = published_version_path
    metadata["finished_at"] = utc_now()
    metadata["error_message"] = None


def mark_run_failure(
    metadata: dict,
    error: Exception,
) -> None:
    """Mark the run failed and capture a diagnostic message."""
    metadata["status"] = "FAILED"
    metadata["finished_at"] = utc_now()
    metadata["error_message"] = f"{type(error).__name__}: {error}"


def append_run_metadata(metadata: dict, output_path: str) -> None:
    """Append one run-metadata record as a JSONL line."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(metadata) + "\n")


def append_run_metadata_safely(
    metadata: dict,
    output_path: str,
) -> None:
    """Attempt to persist metadata without masking pipeline errors."""
    try:
        append_run_metadata(metadata, output_path)
    except Exception as error:
        print(
            "WARNING: Failed to persist run metadata "
            f"for run_id={metadata.get('run_id')}: "
            f"{type(error).__name__}: {error}",
            file=sys.stderr,
        )