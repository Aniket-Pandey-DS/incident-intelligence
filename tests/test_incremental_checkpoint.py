
import json
import pytest
from src.incremental_ingestion import (
    load_checkpoint,
    save_checkpoint,
)


def test_load_checkpoint_returns_empty_list_when_missing(tmp_path):
    assert load_checkpoint(tmp_path / "state.json") == []


def test_save_and_load_checkpoint(tmp_path):
    checkpoint_path = tmp_path / "state.json"

    save_checkpoint(
        checkpoint_path,
        ["batch_0001", "batch_0002"],
    )

    assert load_checkpoint(checkpoint_path) == [
        "batch_0001",
        "batch_0002",
    ]


def test_save_checkpoint_removes_duplicate_batch_ids(tmp_path):
    checkpoint_path = tmp_path / "state.json"

    save_checkpoint(
        checkpoint_path,
        ["batch_0001", "batch_0001", "batch_0002"],
    )

    assert load_checkpoint(checkpoint_path) == [
        "batch_0001",
        "batch_0002",
    ]


def test_load_checkpoint_rejects_invalid_json(tmp_path):
    checkpoint_path = tmp_path / "state.json"
    checkpoint_path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_checkpoint(checkpoint_path)


def test_load_checkpoint_rejects_invalid_structure(tmp_path):
    checkpoint_path = tmp_path / "state.json"
    checkpoint_path.write_text(
        '{"processed_batches": "batch_0001"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="processed_batches"):
        load_checkpoint(checkpoint_path)


def test_save_checkpoint_rejects_empty_batch_ids(tmp_path):
    with pytest.raises(ValueError, match="non-empty string"):
        save_checkpoint(tmp_path / "state.json", ["batch_0001", ""])
