
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_NAMES = [
    "build_silver.py",
    "build_gold.py",
    "build_resolution_performance.py",
]


def test_all_pipelines_use_shared_run_observability():
    for pipeline_name in PIPELINE_NAMES:
        source = (
            PROJECT_ROOT / "pipeline" / pipeline_name
        ).read_text(encoding="utf-8")

        assert "create_run_metadata(" in source
        assert "update_run_counts(" in source
        assert "mark_quality_gate(" in source
        assert "mark_run_success(" in source
        assert "mark_run_failure(" in source
        assert "append_run_metadata_safely(" in source


def test_all_pipelines_have_distinct_run_log_paths():
    expected_log_names = {
        "build_silver.py": "build_silver_runs.jsonl",
        "build_gold.py": "build_gold_runs.jsonl",
        "build_resolution_performance.py":
            "build_resolution_performance_runs.jsonl",
    }

    actual_log_names = []

    for pipeline_name, expected_name in expected_log_names.items():
        source = (
            PROJECT_ROOT / "pipeline" / pipeline_name
        ).read_text(encoding="utf-8")

        assert "RUN_LOG_PATH =" in source
        assert expected_name in source

        actual_log_names.append(expected_name)

    assert len(set(actual_log_names)) == len(expected_log_names)
