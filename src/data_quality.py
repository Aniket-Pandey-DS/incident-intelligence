from pathlib import Path

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_quality_rules(rules_path: str | Path) -> list[dict]:
    """Load data-quality rules from YAML."""
    rules_path = Path(rules_path)

    with rules_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["rules"]


def validate_unique_columns(
    df: DataFrame,
    column: str,
) -> DataFrame:
    """
    Identify duplicate values for a dataset-level uniqueness rule.

    Returns a DataFrame containing the duplicated values and
    their occurrence counts.
    """
    return (
        df.groupBy(column)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.col("count").desc())
    )


def apply_quality_rules(
    df: DataFrame,
    rules: list[dict],
) -> DataFrame:
    """
    Apply configured quality rules and return the DataFrame
    with a `quality_issues` column.
    """

    result = df.withColumn(
        "quality_issues",
        F.array().cast("array<string>"),
    )

    for rule in rules:
        rule_id = rule["id"]
        check = rule["check"]
        column = rule.get("column")

        if check == "not_null":
            condition = F.col(column).isNull()

        elif check == "allowed_values":
            condition = ~F.col(column).isin(rule["values"])

        elif check == "greater_than_or_equal":
            condition = F.col(column) < F.lit(rule["value"])

        elif check == "range":
            condition = (
                (F.col(column) < F.lit(rule["min"]))
                | (F.col(column) > F.lit(rule["max"]))
            )

        elif check == "resolved_at_greater_than_created_at":
            condition = F.col("resolved_at") <= F.col("created_at")

        else:
            raise ValueError(
                f"Unsupported quality check: {check}"
            )

        result = result.withColumn(
            "quality_issues",
            F.when(
                condition,
                F.array_union(
                    F.col("quality_issues"),
                    F.array(F.lit(rule_id)),
                ),
            ).otherwise(F.col("quality_issues")),
        )

    return result