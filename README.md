# Incident Intelligence

A data engineering project that builds a reliable incident-data pipeline using Python, PySpark, schema enforcement, data quality validation, and a Bronze-to-Silver architecture.

## Project Overview

Incident Intelligence processes incident records through a structured data pipeline. It validates incoming data, quarantines invalid records, transforms accepted records into an enriched Silver dataset, and enforces quality checks before publishing the output.

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
      +-----+-----+
      |           |
      v           v
 Valid Records  Invalid Records
      |           |
      v           v
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

- **Schema integrity:** required columns are present.
- **Record-count preservation:** Bronze and Silver record counts match.
- **Duration validity:** resolved incidents have non-negative durations, and unresolved incidents have null durations.
- **Derived-field correctness:** severity priority, creation date, and resolution status flag agree with their source fields.
- **Lifecycle consistency:** incident status, resolution timestamp, and consistency flag follow the defined lifecycle rules.

If a validator detects a violation, it raises an error and prevents the pipeline from reaching the Silver write step.

### 5. Automated Testing

Pytest tests cover schema loading, Bronze data quality, Silver transformations, and Silver quality validation. Tests include both valid scenarios and deliberately invalid inputs.

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
│   └── quarantine/
├── pipeline/
│   ├── ingest_incidents.py
│   └── build_silver.py
├── src/
│   ├── schema_loader.py
│   ├── incident_generator.py
│   ├── data_quality.py
│   ├── silver_transform.py
│   └── silver_quality.py
├── tests/
│   ├── conftest.py
│   ├── test_schema_loader.py
│   ├── test_data_quality.py
│   ├── test_silver_transform.py
│   └── test_silver_quality.py
├── pyproject.toml
└── uv.lock
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

### Run all tests

```bash
pytest -v
```

## Current Verification

The current development dataset has been verified with:

- 100 raw incident records generated.
- 92 valid records accepted into Bronze.
- 8 invalid records quarantined.
- 92 records transformed into Silver.
- 27 columns in the Silver dataset.
- 20 automated tests passing.

These counts describe the current synthetic dataset, not a fixed production expectation.

## Engineering Principles

- Explicit schema enforcement
- Configurable data quality rules
- Separation of transformation and validation logic
- Quarantine of invalid records
- Automated regression testing
- Validation before publishing transformed data
- Reproducible development workflows

## Roadmap

Future work will extend the pipeline with stronger record-level validation, improved publishing and recovery behavior, and additional data engineering capabilities as the project evolves.