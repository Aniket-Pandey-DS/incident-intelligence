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



def test_failed_manifest_publication_preserves_current_version(
    spark: SparkSession,
    tmp_path,
    monkeypatch,
):
    import json
    import src.data_publishing as publishing

    target_path = str(tmp_path / "dataset")

    # Publish Version A.
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

    # Simulate failure when attempting to publish Version B.
    def fail_manifest_write(*args, **kwargs):
        raise OSError("Simulated manifest publication failure")

    monkeypatch.setattr(
        publishing,
        "write_current_manifest",
        fail_manifest_write,
    )

    new_df = spark.createDataFrame(
        [(2, "payments")],
        ["incident_id", "service"],
    )

    with pytest.raises(
        OSError,
        match="Simulated manifest publication failure",
    ):
        publishing.publish_parquet(
            new_df,
            spark,
            target_path,
        )

    # The manifest must still point to Version A.
    current_manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert current_manifest == original_manifest

    # Version A must remain readable.
    published_df = publishing.read_published_parquet(
        spark,
        target_path,
    )

    assert [
        (row["incident_id"], row["service"])
        for row in published_df.collect()
    ] == [(1, "checkout")]

    # Version B was verified, but was never published.
    version_directories = list(
        (tmp_path / "dataset" / "versions").iterdir()
    )

    assert len(version_directories) == 2
    assert first_path in version_directories

    unpublished_paths = [
        path for path in version_directories
        if path != first_path
    ]

    assert len(unpublished_paths) == 1
    assert spark.read.parquet(
        str(unpublished_paths[0])
    ).collect()[0]["incident_id"] == 2



def test_failed_staging_cleans_partial_candidate_and_preserves_current_version(
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

    candidate_id = "simulated_failed_candidate"

    monkeypatch.setattr(
        publishing,
        "generate_version_id",
        lambda: candidate_id,
    )

    def fail_during_staging(df, target, version_id):
        partial_path = (
            tmp_path / "dataset" / "versions" / version_id
        )
        partial_path.mkdir(parents=True)
        (partial_path / "partial.tmp").write_text(
            "incomplete output",
            encoding="utf-8",
        )
        raise OSError("Simulated staging failure")

    monkeypatch.setattr(
        publishing,
        "stage_parquet",
        fail_during_staging,
    )

    new_df = spark.createDataFrame(
        [(2, "payments")],
        ["incident_id", "service"],
    )

    with pytest.raises(
        OSError,
        match="Simulated staging failure",
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
    assert not (
        tmp_path / "dataset" / "versions" / candidate_id
    ).exists()

    version_directories = list(
        (tmp_path / "dataset" / "versions").iterdir()
    )
    assert version_directories == [first_path]



def test_repeated_publication_preserves_dataset_contents(
    spark: SparkSession,
    tmp_path,
):
    import json
    import src.data_publishing as publishing

    target_path = str(tmp_path / "dataset")

    df = spark.createDataFrame(
        [
            (1, "checkout"),
            (2, "payments"),
            (3, "identity"),
        ],
        ["incident_id", "service"],
    )

    # First run.
    first_path = publishing.publish_parquet(
        df,
        spark,
        target_path,
    )

    first_published = publishing.read_published_parquet(
        spark,
        target_path,
    )

    first_rows = {
        tuple(row)
        for row in first_published.collect()
    }

    # Second run with exactly the same input.
    second_path = publishing.publish_parquet(
        df,
        spark,
        target_path,
    )

    second_published = publishing.read_published_parquet(
        spark,
        target_path,
    )

    second_rows = {
        tuple(row)
        for row in second_published.collect()
    }

    manifest = json.loads(
        (tmp_path / "dataset" / "current.json").read_text(
            encoding="utf-8"
        )
    )

    # Each run gets a separate version.
    assert first_path != second_path

    # Both published datasets contain identical business data.
    assert first_rows == second_rows
    assert len(second_rows) == 3

    # The manifest points to the latest successful version.
    assert manifest["version_id"] == second_path.name

    # The previous version remains available.
    assert first_path.is_dir()
    assert second_path.is_dir()
