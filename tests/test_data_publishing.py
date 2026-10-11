import pytest
import json
from pyspark.sql import SparkSession
from src.data_publishing import (
    create_version_directory,
    generate_version_id,
    stage_parquet,
    verify_staged_parquet,
    write_current_manifest,
    publish_parquet,
    read_published_parquet,
)


def test_generate_version_id_returns_unique_ids():
    first_id = generate_version_id()
    second_id = generate_version_id()

    assert first_id != second_id


def test_create_version_directory_creates_directory(tmp_path):
    version_id = generate_version_id()

    version_path = create_version_directory(
        str(tmp_path / "dataset"),
        version_id,
    )

    assert version_path.is_dir()
    assert version_path.name == version_id

def test_create_version_directory_rejects_existing_directory(tmp_path):
    target_path = str(tmp_path / "dataset")
    version_id = generate_version_id()

    create_version_directory(target_path, version_id)

    with pytest.raises(FileExistsError):
        create_version_directory(target_path, version_id)


def test_stage_parquet_writes_dataset(spark: SparkSession, tmp_path):
    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )

    version_id = generate_version_id()

    version_path = stage_parquet(
        df,
        str(tmp_path / "dataset"),
        version_id,
    )

    assert version_path.is_dir()
    assert spark.read.parquet(str(version_path)).count() == 2

def test_verify_staged_parquet_passes(spark: SparkSession, tmp_path):
    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )

    version_path = tmp_path / "staged"
    df.write.parquet(str(version_path))

    verify_staged_parquet(
        spark,
        str(version_path),
        expected_count=2,
    )

def test_verify_staged_parquet_passes(spark: SparkSession, tmp_path):
    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )

    version_path = tmp_path / "staged"
    df.write.parquet(str(version_path))

    verify_staged_parquet(
        spark,
        str(version_path),
        expected_count=2,
    )

def test_verify_staged_parquet_fails_on_count_mismatch(
    spark: SparkSession,
    tmp_path,
):
    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )

    version_path = tmp_path / "staged"
    df.write.parquet(str(version_path))

    with pytest.raises(
        ValueError,
        match="Staged row count mismatch",
    ):
        verify_staged_parquet(
            spark,
            str(version_path),
            expected_count=3,
        )



def test_write_current_manifest_creates_manifest(tmp_path):
    target_path = tmp_path / "dataset"
    version_id = generate_version_id()

    manifest_path = write_current_manifest(
        str(target_path),
        version_id,
    )

    assert manifest_path.is_file()

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert manifest == {
        "version_id": version_id,
        "path": f"versions/{version_id}",
    }


def test_write_current_manifest_replaces_previous_version(
    tmp_path,
):
    target_path = str(tmp_path / "dataset")
    first_version = generate_version_id()
    second_version = generate_version_id()

    write_current_manifest(target_path, first_version)
    write_current_manifest(target_path, second_version)

    manifest_path = tmp_path / "dataset" / "current.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert manifest["version_id"] == second_version
    assert manifest["path"] == f"versions/{second_version}"




def test_publish_parquet_publishes_verified_version(
    spark: SparkSession,
    tmp_path,
):
    import json

    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )
    target_path = str(tmp_path / "dataset")

    version_path = publish_parquet(
        df,
        spark,
        target_path,
    )

    manifest_path = tmp_path / "dataset" / "current.json"
    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert version_path.is_dir()
    assert manifest["version_id"] == version_path.name
    assert spark.read.parquet(str(version_path)).count() == 2


def test_failed_verification_preserves_current_version(
    spark: SparkSession,
    tmp_path,
    monkeypatch,
):
    import json
    import src.data_publishing as publishing

    target_path = str(tmp_path / "dataset")

    original_df = spark.createDataFrame(
        [(1, "checkout")],
        ["incident_id", "service"],
    )

    first_path = publishing.publish_parquet(
        original_df,
        spark,
        target_path,
    )

    manifest_path = tmp_path / "dataset" / "current.json"
    original_manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    def fail_verification(*args, **kwargs):
        raise ValueError("Simulated verification failure")

    monkeypatch.setattr(
        publishing,
        "verify_staged_parquet",
        fail_verification,
    )

    new_df = spark.createDataFrame(
        [(2, "payments")],
        ["incident_id", "service"],
    )

    with pytest.raises(
        ValueError,
        match="Simulated verification failure",
    ):
        publishing.publish_parquet(
            new_df,
            spark,
            target_path,
        )

    current_manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert current_manifest == original_manifest
    assert first_path.is_dir()

    version_directories = list(
        (tmp_path / "dataset" / "versions").iterdir()
    )
    assert version_directories == [first_path]






def test_read_published_parquet_reads_current_version(
    spark: SparkSession,
    tmp_path,
):
    df = spark.createDataFrame(
        [(1, "checkout"), (2, "payments")],
        ["incident_id", "service"],
    )
    target_path = str(tmp_path / "dataset")

    published_path = publish_parquet(
        df,
        spark,
        target_path,
    )

    result = read_published_parquet(spark, target_path)

    assert result.count() == 2
    assert set(result.columns) == {"incident_id", "service"}
    assert {
        row["service"] for row in result.collect()
    } == {"checkout", "payments"}


def test_read_published_parquet_fails_without_manifest(
    spark: SparkSession,
    tmp_path,
):
    target_path = str(tmp_path / "dataset")

    with pytest.raises(
        FileNotFoundError,
        match="No published manifest found",
    ):
        read_published_parquet(spark, target_path)


def test_read_published_parquet_rejects_invalid_manifest_path(
    spark: SparkSession,
    tmp_path,
):
    import json

    target_dir = tmp_path / "dataset"
    target_dir.mkdir()

    manifest = {
        "version_id": "version-a",
        "path": "versions/../../outside",
    }

    (target_dir / "current.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Manifest path does not match",
    ):
        read_published_parquet(spark, str(target_dir))
