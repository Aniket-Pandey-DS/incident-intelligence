# Incident Intelligence

A data engineering project that builds a reliable incident-data pipeline using Python, PySpark, schema enforcement, data quality validation, and a Bronze-to-Silver-to-Gold architecture.

## Project Overview

Incident Intelligence processes incident records through a structured data pipeline. It validates incoming data, quarantines invalid records, transforms accepted records into an enriched Silver dataset, and aggregates Silver data into business-ready Gold analytics.

The project is being developed incrementally, with a focus on production-oriented data engineering practices.

## Architecture

```text
Synthetic Incident Generator
            |
            v
       Raw CSV Data
            |
            v
     Schema Enforcement
            |
            v
   Bronze Ingestion Pipeline
            |
       +----+----+
       |         |
       v         v
 Valid Records  Invalid Records
       |         |
       v         v
 Bronze Parquet  Quarantine Parquet
       |
       v
 Silver Transformation
       |
       v
 Silver Quality Gate
       |
       v
  Silver Parquet
       |
       v
 Gold Aggregation
       |
       v
 Gold Quality Gate
       |
       v
 Gold Incident Summary
```

## Current Implementation

### 1. Synthetic Incident Data

- Generates incident records with realistic operational fields.
- Includes deliberately injected data quality issues for validation testing.
- Uses a fixed random seed for reproducible generation.

### 2. Bronze Ingestion

- Loads the incident schema from YAML.
- Reads raw CSV data using an explicit PySpark schema.
- Applies configurable data quality rules.
- Separates valid records from invalid records.
- Writes accepted records to Bronze Parquet and rejected records to quarantine Parquet.

### 3. Silver Transformation

The Silver pipeline enriches validated Bronze records with derived fields:

- `incident_duration_minutes`
- `severity_priority`
- `is_resolved`
- `created_date`
- `resolution_status_consistent`

It also trims whitespace from selected text fields, including `service` and `region`.

### 4. Silver Data Quality Gate

Before writing Silver output, the pipeline validates:

- **Schema integrity:** Required columns are present.
- **Record-count preservation:** Bronze and Silver record counts match.
- **Duration validity:** Resolved incidents have non-negative durations, and unresolved incidents have null durations.
- **Derived-field correctness:** Severity priority, creation date, and resolution status flag agree with their source fields.
- **Lifecycle consistency:** Incident status, resolution timestamp, and consistency flag follow the defined lifecycle rules.

If a validator detects a violation, it raises an error and prevents the pipeline from reaching the Silver write step.

### 5. Gold Analytics Layer

The Gold pipeline aggregates Silver incidents into a business-ready summary for reporting and downstream analysis.

**Gold Incident Summary grain:** One row per `created_date`, `service`, `region`, and `severity`.

The summary contains eight columns:

| Column | Description |
|---|---|
| `created_date` | Date the incident was created |
| `service` | Affected service |
| `region` | Affected region |
| `severity` | Incident severity |
| `incident_count` | Total incidents in the group |
| `resolved_count` | Incidents marked resolved |
| `open_count` | Incidents marked open |
| `avg_duration_minutes` | Average available incident duration in minutes |

The average duration excludes null values. Groups with no available duration have a null average rather than a misleading value of zero.

### 6. Gold Data Quality Gate

Before writing Gold output, the pipeline validates:

- **Schema integrity:** All eight required columns are present.
- **Incident-count preservation:** The sum of Gold `incident_count` values matches the Silver record count.
- **Group-count reconciliation:** Resolved and open counts are non-negative and sum to each group's total.
- **Unique grain:** No duplicate rows exist for the defined four-column grouping grain.

The Gold write step runs only after these validations succeed.

### 7. Automated Testing

Pytest tests cover:

- Schema loading
- Bronze data quality
- Silver transformations
- Silver quality validation
- Gold transformations
- Gold quality validation

Tests include valid scenarios, deliberately invalid inputs, count reconciliation, duplicate-grain detection, and handling of null values.

## Technology Stack

- Python 3.12
- PySpark 4.2
- Apache Spark
- YAML
- Parquet
- pytest
- uv for Python environment and dependency management

## Project Structure

```text
incident-intelligence/
├── configs/
│   ├── incident_schema.yaml
│   └── incident_quality_rules.yaml
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
├── pipeline/
│   ├── ingest_incidents.py
│   ├── build_silver.py
│   └── build_gold.py
├── src/
│   ├── schema_loader.py
│   ├── incident_generator.py
│   ├── data_quality.py
│   ├── silver_transform.py
│   ├── silver_quality.py
│   ├── gold_transform.py
│   └── gold_quality.py
├── tests/
│   ├── conftest.py
│   ├── test_schema_loader.py
│   ├── test_data_quality.py
│   ├── test_silver_transform.py
│   ├── test_silver_quality.py
│   ├── test_gold_transform.py
│   └── test_gold_quality.py
├── pyproject.toml
├── uv.lock
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.12
- Java 21
- `uv`

### Install dependencies

```bash
uv sync
source .venv/bin/activate
```

### Run the Bronze ingestion pipeline

```bash
python -m pipeline.ingest_incidents
```

### Build the Silver dataset

```bash
python -m pipeline.build_silver
```

### Build the Gold analytics dataset

```bash
python -m pipeline.build_gold
```

### Run all tests

```bash
pytest -v
```

## Current Verification

The current synthetic dataset and pipeline have been verified with:

- 100 raw incident records generated.
- 92 valid records accepted into Bronze.
- 8 invalid records quarantined.
- 92 records transformed into Silver.
- 27 columns in the Silver dataset.
- 89 aggregated rows produced in Gold.
- 8 columns in the Gold Incident Summary.
- 31 automated tests passing.

These counts describe the current synthetic dataset, not fixed production expectations. Gold row counts depend on the distinct combinations of creation date, service, region, and severity.

## Engineering Principles

- Explicit schema enforcement
- Configurable data quality rules
- Separation of transformation and validation logic
- Quarantine of invalid records
- Automated regression testing
- Validation before publishing transformed data
- Reproducible development workflows
- Layered data architecture with clear responsibilities

## Roadmap

Future work will extend the pipeline with stronger record-level validation, safer publishing and recovery behavior, additional Gold analytics datasets, and other data engineering capabilities as the project evolves.