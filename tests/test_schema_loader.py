from pathlib import Path

from pyspark.sql.types import *
from pyspark.sql.functions import *

from src.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "configs" / "incident_schema.yaml"


def test_load_incident_schema():
    schema = load_schema(SCHEMA_PATH)

    assert isinstance(schema, StructType)

    field_names = schema.fieldNames()

    assert len(field_names) == 22
    assert "incident_id" in field_names
    assert "created_at" in field_names
    assert "severity" in field_names
    assert "error_rate" in field_names
    assert "ingested_at" in field_names


def test_incident_schema_field_types():
    schema = load_schema(SCHEMA_PATH)

    fields = {field.name: field.dataType for field in schema.fields}

    assert isinstance(fields["incident_id"], StringType)
    assert isinstance(fields["created_at"], TimestampType)
    assert isinstance(fields["affected_users"], IntegerType)
    assert isinstance(fields["error_rate"], DoubleType)