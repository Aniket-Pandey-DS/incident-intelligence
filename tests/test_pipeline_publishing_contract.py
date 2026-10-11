from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_silver_pipeline_uses_shared_publisher_and_reader():
    source = (
        PROJECT_ROOT / "pipeline" / "build_silver.py"
    ).read_text(encoding="utf-8")

    assert "publish_parquet(" in source
    assert "read_published_parquet(" in source
    assert 'silver_df.write.mode("overwrite").parquet' not in source


def test_gold_pipelines_use_published_silver_and_shared_publisher():
    pipeline_names = [
        "build_gold.py",
        "build_resolution_performance.py",
    ]

    for pipeline_name in pipeline_names:
        source = (
            PROJECT_ROOT / "pipeline" / pipeline_name
        ).read_text(encoding="utf-8")

        assert "read_published_parquet(" in source
        assert "publish_parquet(" in source
        assert 'spark.read.parquet(str(SILVER_PATH))' not in source
