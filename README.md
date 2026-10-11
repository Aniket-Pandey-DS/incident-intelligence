# Incident Intelligence

A data engineering project that builds a reliable incident-data pipeline using Python, PySpark, schema enforcement, data quality validation, and a Bronze-to-Silver-to-Gold architecture.

## Project Overview

Incident Intelligence processes incident records through a structured data pipeline. It validates incoming data, quarantines invalid records, transforms accepted records into an enriched Silver dataset, and produces business-ready Gold analytics.

The project focuses on production-oriented data engineering practices, including explicit schemas, configurable data quality rules, automated testing, versioned dataset publishing, manifest-aware reads, and run-level observability.

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
 Silver Versioned Publishing
      |
      v
 Manifest-Aware Silver Reader
      |
      +----------------------------+
      |                            |
      v                            v
 Gold Incident Summary      Gold Resolution Performance
      |                            |
      v                            v
 Gold Quality Gate          Gold Quality Gate
      |                            |
      v                            v
 Versioned Gold Output      Versioned Gold Output
      |                            |
      +-------------+--------------+
                    |
                    v
         Run-Level Observability
         (JSONL execution records)
```

Each pipeline validates its output before publishing a new version. Downstream Gold pipelines resolve the published Silver dataset through its manifest rather than relying on a hard-coded version directory. The Silver and Gold pipelines also record execution metadata in separate JSONL logs.

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
- Detects duplicate incident IDs and quarantines records that violate the configured rules.

### 3. Silver Transformation

The Silver pipeline enriches validated Bronze records with derived fields:

- `incident_duration_minutes`
- `severity_priority`
- `is_resolved`
- `created_date`
- `resolution_status_consistent`

It also trims whitespace from selected text fields, including `service` and `region`.

The Silver dataset preserves the original 22 columns and adds five derived columns, resulting in 27 columns.

### 4. Silver Data Quality Gate

Before publishing Silver output, the pipeline validates:

- **Schema integrity:** Required columns are present.
- **Record-count preservation:** Bronze and Silver record counts match.
- **Duration validity:** Resolved incidents have non-negative durations, and unresolved incidents have null durations.
- **Derived-field correctness:** Severity priority, creation date, and resolution status consistency agree with their source fields.
- **Lifecycle consistency:** Incident status, resolution timestamp, and the consistency flag follow the defined lifecycle rules.

If a validator detects a violation, it raises an error and prevents the candidate Silver dataset from being published.

### 5. Gold Analytics Layer

The Gold layer contains two independently built analytical datasets.

#### Gold Incident Summary

**Grain:** One row per `created_date`, `service`, `region`, and `severity`.

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

#### Gold Resolution Performance

**Grain:** One row per `resolution_date`, `service`, and `severity`.

This dataset focuses on resolved incidents with a non-null resolution timestamp.

| Column | Description |
|---|---|
| `resolution_date` | Date the incident was resolved |
| `service` | Affected service |
| `severity` | Incident severity |
| `resolved_incident_count` | Number of resolved incidents in the group |
| `duration_observation_count` | Number of valid duration observations used for duration metrics |
| `avg_resolution_minutes` | Average resolution duration in minutes |
| `median_resolution_minutes` | Approximate median resolution duration in minutes |
| `max_resolution_minutes` | Maximum resolution duration in minutes |

The median is calculated using PySpark's `percentile_approx` function. This provides an approximate percentile rather than an exact median calculation.

### 6. Gold Data Quality Gates

Both Gold pipelines validate their output before publishing it.

**Incident Summary validations:**

- **Schema integrity:** All eight required columns are present.
- **Incident-count reconciliation:** The sum of Gold `incident_count` values matches the Silver record count.
- **Group-count reconciliation:** Resolved and open counts are non-negative and sum to each group's total.
- **Unique grain:** No duplicate rows exist for the defined four-column grouping grain.

**Resolution Performance validations:**

- **Schema integrity:** All eight required columns are present.
- **Resolution-count reconciliation:** Aggregated resolved incident counts match the eligible resolved Silver records.
- **Metric validity:** Resolution-duration metrics satisfy the defined validity checks.
- **Unique grain:** No duplicate rows exist for the defined three-column grouping grain.

A failed validation prevents the candidate Gold dataset from becoming the published version.

### 7. Versioned Dataset Publishing

Silver and both Gold pipelines use a shared publishing module implemented in `src/data_publishing.py`.

Instead of overwriting a single Parquet directory directly, each successful publication creates a new version under a `versions/` directory.

**Publishing workflow:**

1. Generate a unique version ID using a UTC timestamp and UUID suffix.
2. Write candidate Parquet data to a new version directory.
3. Read the staged dataset back and verify its row count.
4. Update the `current.json` manifest only after verification succeeds.
5. Allow downstream readers to resolve the currently published version through the manifest.

Example directory structure:

```text
data/
├── silver/
│   └── incidents/
│       ├── current.json
│       └── versions/
│           └── <version_id>/
│               ├── part-*.parquet
│               └── _SUCCESS
└── gold/
    ├── incident_summary/
    │   ├── current.json
    │   └── versions/
    │       └── <version_id>/
    └── resolution_performance/
        ├── current.json
        └── versions/
            └── <version_id>/
```

The manifest identifies the currently published version:

```json
{
  "version_id": "<version_id>",
  "path": "versions/<version_id>"
}
```

The actual version ID is generated at runtime.

**Failure handling:**

- If staging or row-count verification fails, the failed candidate version is removed.
- If candidate verification fails, the existing manifest remains unchanged.
- If manifest publication fails, the verified candidate is retained for inspection; it may remain unpublished.
- Previously published versions are retained rather than overwritten.
- The manifest-aware reader validates the manifest structure, checks that the relative path matches the version ID, and verifies that the referenced directory exists before reading Parquet.

**Important limitations:** This implementation targets a single publisher on a local filesystem. Atomic manifest replacement relies on local filesystem semantics. Concurrent publishers are not coordinated with locking or compare-and-swap, and cloud object storage requires storage-specific publication and consistency guarantees.

### 8. Run-Level Observability

The three transformation pipelines use the shared observability module in `src/run_observability.py`. Each run creates a unique run ID and records execution metadata in a JSON Lines (JSONL) file.

| Pipeline | Run log |
|---|---|
| Silver | `data/observability/build_silver_runs.jsonl` |
| Gold Incident Summary | `data/observability/build_gold_runs.jsonl` |
| Gold Resolution Performance | `data/observability/build_resolution_performance_runs.jsonl` |

Each record includes:

- `run_id` — unique identifier for the execution.
- `pipeline_name` — pipeline that ran.
- `status` — `RUNNING`, `SUCCESS`, or `FAILED` as the run progresses.
- `started_at` and `finished_at` — UTC timestamps.
- `input_records` and `output_records` — row counts when available. For Gold pipelines, the output count is the number of aggregated Gold rows.
- `quality_gate_passed` — quality-gate outcome, or `null` if the gate did not complete.
- `published_version_path` — published version path after successful publication and read-back verification.
- `error_message` — exception type and message when a run fails.

Run records are appended rather than replacing previous records. A safe logging wrapper reports a warning if a JSONL write fails, avoiding masking an existing pipeline exception. If logging fails after an otherwise successful pipeline run, the pipeline may still succeed without a persisted run record.

**Operational limitations:** These are local JSONL logs, not a centralized logging service or transactional audit store. Concurrent-writer coordination, guaranteed log persistence during filesystem failures, alerting, and log rotation are not implemented yet.

### 9. Automated Testing

Pytest tests cover:

- Schema loading
- Bronze data quality
- Silver transformations
- Silver quality validation
- Gold transformations
- Gold quality validation
- Resolution Performance aggregations and quality checks
- Versioned publishing and manifest handling
- Published dataset read-back and failure recovery
- Pipeline publishing contracts
- Run metadata generation and lifecycle updates
- JSONL persistence and safe logging behavior
- Pipeline observability contracts
- Shared Spark test-session behavior

Tests include valid scenarios, deliberately invalid inputs, count reconciliation, duplicate-grain detection, null handling, manifest replacement, preservation of the previously published version when candidate verification fails, and structural checks that all three pipelines use the shared observability functions.

**Current test suite: 76 passing tests.**

## Technology Stack

- Python 3.12
- PySpark 4.2
- Apache Spark
- YAML
- Parquet
- pytest
- `uv` for Python environment and dependency management
- Java 21

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
│   │   ├── incident_summary/
│   │   └── resolution_performance/
│   ├── quarantine/
│   └── observability/
├── pipeline/
│   ├── ingest_incidents.py
│   ├── build_silver.py
│   ├── build_gold.py
│   └── build_resolution_performance.py
├── src/
│   ├── schema_loader.py
│   ├── incident_generator.py
│   ├── data_quality.py
│   ├── silver_transform.py
│   ├── silver_quality.py
│   ├── gold_transform.py
│   ├── gold_quality.py
│   ├── data_publishing.py
│   └── run_observability.py
├── tests/
│   ├── conftest.py
│   ├── test_schema_loader.py
│   ├── test_data_quality.py
│   ├── test_silver_transform.py
│   ├── test_silver_quality.py
│   ├── test_gold_transform.py
│   ├── test_gold_quality.py
│   ├── test_gold_resolution_performance.py
│   ├── test_data_publishing.py
│   ├── test_pipeline_publishing_contract.py
│   ├── test_run_observability.py
│   └── test_pipeline_observability_contract.py
├── pyproject.toml
├── uv.lock
└── README.md
```

Generated datasets and observability logs are local development artifacts and are excluded from Git through `.gitignore`.

## Getting Started

### Prerequisites

- Python 3.12
- Java 21
- `uv`

### Install dependencies

Run the following commands from the project root:

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

### Build the Gold Incident Summary

```bash
python -m pipeline.build_gold
```

### Build Gold Resolution Performance

```bash
python -m pipeline.build_resolution_performance
```

### Run all tests

```bash
pytest -v
```

### Inspect the latest run records

```bash
tail -n 1 data/observability/build_silver_runs.jsonl
tail -n 1 data/observability/build_gold_runs.jsonl
tail -n 1 data/observability/build_resolution_performance_runs.jsonl
```

These commands show the most recently appended run record for each pipeline.

## Current Verification

The current synthetic dataset and pipeline have been verified with:

- **100** raw incident records.
- **92** valid records accepted into Bronze.
- **8** invalid records quarantined.
- **92** records transformed into Silver.
- **27** columns in Silver.
- **89** aggregated rows in Gold Incident Summary.
- **8** columns in Gold Incident Summary.
- **64** aggregated rows in Gold Resolution Performance in the verified run.
- **8** columns in Gold Resolution Performance.
- **76** automated tests passing.
- Successful JSONL run records observed for Silver and both Gold pipelines.

These counts describe the current synthetic dataset and verified runs, not fixed production expectations. Gold row counts depend on the data and the distinct combinations of each table's grouping dimensions. Version IDs and publication paths are generated dynamically.

## Engineering Principles

- Explicit schema enforcement
- Configurable data quality rules
- Separation of transformation and validation logic
- Quarantine of invalid records
- Automated regression testing
- Validation before publication
- Versioned datasets and manifest-aware readers
- Run-level observability with execution IDs and status records
- Reproducible development workflows
- Layered data architecture with clear responsibilities
- Explicit documentation of failure modes and operational limitations

## Roadmap

The next stage is to strengthen operational reliability through controlled failure scenarios and deeper idempotency testing. Planned improvements include verifying that previously published datasets remain available after failures, testing failure handling at different pipeline stages, and reviewing retry behavior. Later stages can introduce orchestration and additional downstream analytics while keeping data engineering reliability as the foundation.
