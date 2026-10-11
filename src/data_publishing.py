from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from uuid import uuid4
from pathlib import Path
import json
import os
import tempfile
import shutil

# Creating an identifier
def generate_version_id() -> str:
    """Generate a unique UTC-based dataset version ID."""
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    unique_suffix = uuid4().hex[:8]

    return f"{timestamp}_{unique_suffix}"

# Creating the storage location
def create_version_directory(
    target_path: str,
    version_id: str,
) -> Path:
    """Create and return a directory for a dataset version."""
    versions_path = Path(target_path) / "versions"
    version_path = versions_path / version_id

    version_path.mkdir(parents=True, exist_ok=False)

    return version_path


# Staging the data
def stage_parquet(
    df: DataFrame,
    target_path: str,
    version_id: str,
) -> Path:
    """Write a candidate Parquet dataset to a new version directory."""
    version_path = (
        Path(target_path) / "versions" / version_id
    )

    version_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.write.mode("errorifexists").parquet(str(version_path))

    return version_path

# Verifying the staged data
def verify_staged_parquet(
    spark: SparkSession,
    version_path: str,
    expected_count: int,
) -> None:
    """Verify that staged Parquet data is readable and has the expected row count."""
    staged_df = spark.read.parquet(version_path)
    actual_count = staged_df.count()

    if actual_count != expected_count:
        raise ValueError(
            f"Staged row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )


# Manifest writer
def write_current_manifest(
    target_path: str,
    version_id: str,
) -> Path:
    """Atomically update the manifest to point to a published version."""
    target_dir = Path(target_path)
    target_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = target_dir / "current.json"
    manifest = {
        "version_id": version_id,
        "path": f"versions/{version_id}",
    }

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target_dir,
            prefix=".current.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(manifest, temp_file, indent=2)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        os.replace(temp_path, manifest_path)

    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()

    return manifest_path



# Publishing the data
def publish_parquet(
    df: DataFrame,
    spark: SparkSession,
    target_path: str,
) -> Path:
    """Publish a verified Parquet version and update its manifest."""
    expected_count = df.count()
    version_id = generate_version_id()

    version_path = (
        Path(target_path) / "versions" / version_id
    )

    # Avoid accidentally reusing an existing version directory.
    if version_path.exists():
        raise FileExistsError(
            f"Version already exists: {version_path}"
        )

    try:
        staged_path = stage_parquet(
            df,
            target_path,
            version_id,
        )

        verify_staged_parquet(
            spark,
            str(staged_path),
            expected_count,
        )

    except Exception:
        # Remove only this failed candidate.
        if version_path.exists():
            shutil.rmtree(version_path)
        raise

    # Publish only after staging and verification succeed.
    # If manifest publication fails, keep the verified candidate:
    # it is unpublished and does not replace the current version.
    write_current_manifest(
        target_path,
        version_id,
    )

    return version_path



def read_published_parquet(
    spark: SparkSession,
    target_path: str,
) -> DataFrame:
    """Read the Parquet version referenced by the current manifest."""
    target_dir = Path(target_path)
    manifest_path = target_dir / "current.json"

    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"No published manifest found: {manifest_path}"
        )

    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    version_id = manifest.get("version_id")
    relative_path = manifest.get("path")

    if not isinstance(version_id, str) or not version_id:
        raise ValueError("Manifest contains an invalid version_id")

    expected_relative_path = f"versions/{version_id}"

    if relative_path != expected_relative_path:
        raise ValueError(
            "Manifest path does not match its version_id"
        )

    version_path = target_dir / relative_path

    # Ensure the resolved path stays inside the dataset directory.
    resolved_target = target_dir.resolve()
    resolved_version = version_path.resolve()

    if not resolved_version.is_relative_to(resolved_target):
        raise ValueError("Manifest path escapes the dataset directory")

    if not resolved_version.is_dir():
        raise FileNotFoundError(
            f"Published version directory not found: {resolved_version}"
        )

    return spark.read.parquet(str(resolved_version))
