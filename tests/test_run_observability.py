from datetime import datetime
from uuid import UUID
import json

from src.run_observability import (
    append_run_metadata,
    create_run_metadata, 
    utc_now,
    update_run_counts,
    mark_quality_gate,
    mark_run_success,
    mark_run_failure,
    append_run_metadata_safely,
)
import src.run_observability as observability


def test_utc_now_returns_valid_utc_timestamp():
    timestamp = utc_now()

    parsed = datetime.fromisoformat(timestamp)

    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0


def test_create_run_metadata_has_expected_initial_state():
    metadata = create_run_metadata("build_silver")

    assert metadata["pipeline_name"] == "build_silver"
    assert metadata["status"] == "RUNNING"
    assert metadata["finished_at"] is None
    assert metadata["input_records"] is None
    assert metadata["output_records"] is None
    assert metadata["quality_gate_passed"] is None
    assert metadata["published_version_path"] is None
    assert metadata["error_message"] is None


def test_create_run_metadata_generates_valid_run_id():
    metadata = create_run_metadata("build_silver")

    parsed_id = UUID(metadata["run_id"])

    assert str(parsed_id) == metadata["run_id"]


def test_create_run_metadata_generates_unique_run_ids():
    first = create_run_metadata("build_silver")
    second = create_run_metadata("build_silver")

    assert first["run_id"] != second["run_id"]


def test_update_run_counts_records_counts():
    metadata = create_run_metadata("build_silver")

    update_run_counts(metadata, 100, 92)

    assert metadata["input_records"] == 100
    assert metadata["output_records"] == 92


def test_update_run_counts_rejects_negative_counts():
    metadata = create_run_metadata("build_silver")

    try:
        update_run_counts(metadata, -1, 92)
        assert False, "Expected ValueError for negative counts"
    except ValueError:
        pass


def test_mark_run_success_records_publication():
    metadata = create_run_metadata("build_silver")

    mark_run_success(metadata, "data/silver/incidents/versions/example")

    assert metadata["status"] == "SUCCESS"
    assert metadata["published_version_path"] == (
        "data/silver/incidents/versions/example"
    )
    assert metadata["finished_at"] is not None
    assert metadata["error_message"] is None


def test_mark_run_failure_records_exception():
    metadata = create_run_metadata("build_silver")

    mark_run_failure(metadata, ValueError("Invalid incident data"))

    assert metadata["status"] == "FAILED"
    assert metadata["finished_at"] is not None
    assert metadata["error_message"] == (
        "ValueError: Invalid incident data"
    )


def test_append_run_metadata_writes_jsonl_record(tmp_path):
    metadata = create_run_metadata("build_silver")
    log_path = tmp_path / "observability" / "runs.jsonl"

    append_run_metadata(metadata, str(log_path))

    assert log_path.exists()

    records = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]

    assert len(records) == 1
    assert records[0]["run_id"] == metadata["run_id"]
    assert records[0]["pipeline_name"] == "build_silver"


def test_append_run_metadata_preserves_previous_records(tmp_path):
    metadata1 = create_run_metadata("build_silver")
    metadata2 = create_run_metadata("build_gold")
    log_path = tmp_path / "runs.jsonl"

    append_run_metadata(metadata1, str(log_path))
    append_run_metadata(metadata2, str(log_path))

    records = [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
    ]

    assert len(records) == 2
    assert records[0]["run_id"] == metadata1["run_id"]
    assert records[1]["run_id"] == metadata2["run_id"]


def test_failed_run_can_be_recorded_to_jsonl(tmp_path):
    metadata = create_run_metadata("build_silver")

    error = ValueError("Invalid incident data")
    mark_run_failure(metadata, error)

    log_path = tmp_path / "runs.jsonl"
    append_run_metadata(metadata, str(log_path))

    import json

    record = json.loads(log_path.read_text(encoding="utf-8"))

    assert record["status"] == "FAILED"
    assert record["finished_at"] is not None
    assert record["error_message"] == "ValueError: Invalid incident data"
    assert record["quality_gate_passed"] is None



def test_safe_append_warns_instead_of_raising_on_log_failure(
    tmp_path, monkeypatch, capsys
):
    metadata = create_run_metadata("build_silver")

    def fail_to_write(metadata, output_path):
        raise OSError("Disk unavailable")

    monkeypatch.setattr(
        observability,
        "append_run_metadata",
        fail_to_write,
    )

    append_run_metadata_safely(
        metadata,
        str(tmp_path / "runs.jsonl"),
    )

    captured = capsys.readouterr()

    assert "WARNING" in captured.err
    assert metadata["run_id"] in captured.err
    assert "Disk unavailable" in captured.err
