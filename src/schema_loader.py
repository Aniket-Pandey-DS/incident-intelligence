from pathlib import Path

import yaml
from pyspark.sql.types import *
from pyspark.sql.functions import *


TYPE_MAPPING = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


def load_schema(schema_path: str | Path) -> StructType:
    """
    Load a YAML data contract and convert it into a PySpark StructType.
    """
    schema_path = Path(schema_path)

    with schema_path.open("r", encoding="utf-8") as file:
        schema_definition = yaml.safe_load(file)

    fields = []

    for field in schema_definition["fields"]:
        field_type = field["type"]

        if field_type not in TYPE_MAPPING:
            raise ValueError(
                f"Unsupported field type '{field_type}' "
                f"for field '{field['name']}'"
            )

        fields.append(
            StructField(
                field["name"],
                TYPE_MAPPING[field_type],
                field["nullable"],
            )
        )

    return StructType(fields)