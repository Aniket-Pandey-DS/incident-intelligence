# Incident Intelligence

A data engineering portfolio project for building a reliable
incident-data platform with Python, PySpark, schema enforcement, data
quality validation, incremental ingestion, versioned publishing, and a
Bronze--Silver--Gold architecture.

## Project Overview

Incident Intelligence ingests operational incident records, validates
them against a YAML-defined data contract, quarantines invalid records,
enriches accepted records in Silver, and produces analytical Gold
datasets.

The project emphasizes practical data engineering patterns:

-   Explicit schema enforcement and configurable quality rules
-   Bronze, Silver, and Gold data layers
-   Incremental batch ingestion with insert/update merge semantics
-   Batch checkpoints and replay handling
-   Versioned Parquet publishing with manifest-aware reads
-   Quality gates before publication
-   Run-level observability for Silver and Gold transformations
-   Automated regression and failure-path tests

> **Implementation scope:** this is a local PySpark implementation
> designed to demonstrate engineering patterns. It is not a distributed,
> multi-writer production deployment. Limitations are documented below.

## Architecture

``` text
                         Raw CSV
                            |
                            v
                  YAML Schema + Quality Rules
                            |
              +-------------+-------------+
              |                           |
              v                           v
       Valid Bronze Records       Invalid Records
              |                           |
              v                           v
      Versioned Bronze Parquet       Quarantine
              |
              v
      Silver Transformation
              |
              v
        Silver Quality Gate
              |
              v
      Versioned Silver Parquet
              |
              v
          +---+--------------------+
          |                        |
          v                        v
  Gold Incident Summary    Gold Resolution Performance
          |                        |
          v                        v
    Gold Quality Gate         Gold Quality Gate
          |                        |
          v                        v
  Versioned Gold Output      Versioned Gold Output

Incremental path:
Incoming CSV batch -> Validate -> Quarantine invalid rows
                   -> Merge with current Bronze snapshot
                   -> Publish verified Bronze version
                   -> Save processed-batch checkpoint
```

## Current Implementation

### 1. Synthetic incident data

-   Generates incident records with operational fields.
-   Includes deliberately injected data-quality issues.
-   Uses a fixed random seed for reproducible generation.

### 2. Full-load Bronze ingestion

The full-load ingestion pipeline:

-   Loads the canonical schema from `configs/incident_schema.yaml`.
-   Reads the raw CSV using an explicit PySpark schema.
-   Applies rules from `configs/incident_quality_rules.yaml`.
-   Separates valid records from invalid records.
-   Publishes valid records as versioned Bronze Parquet.
-   Writes invalid records to quarantine.
-   Detects duplicate incident IDs and applies the configured quality
    rules.

Run it with:

``` bash
python pipeline/ingest_incidents.py
```

The full-load pipeline is separate from the incremental test dataset
described below.

### 3. Incremental batch ingestion

The incremental runner is `pipeline/process_incident_batch.py`. It
accepts a batch ID, loads the corresponding CSV from
`data/incoming/incidents/`, applies the canonical schema and quality
rules, merges valid records into the current incremental Bronze
snapshot, publishes a new version, and then records the batch ID in a
JSON checkpoint.

Run an individual batch from the repository root:

``` bash
python pipeline/process_incident_batch.py batch_0001
```

The input file must be named `data/incoming/incidents/<batch_id>.csv`
and contain the full 22-column incident schema.

Example sample batches:

``` text
data/incoming/incidents/
├── batch_0001.csv
├── batch_0002.csv
└── batch_0003.csv
```

The sample sequence demonstrates:

-   `batch_0001`: inserts two valid incidents.
-   `batch_0002`: updates an existing incident and inserts a new
    incident.
-   `batch_0003`: contains one valid incident and one invalid incident;
    the invalid severity is quarantined.

The incremental runner uses separate paths so the demonstration does not
overwrite the full-load Bronze dataset:

  Purpose                        Path
  ------------------------------ ------------------------------------------
  Incoming batch files           `data/incoming/incidents/`
  Incremental Bronze snapshots   `data/bronze/incremental_incidents/`
  Incremental quarantine         `data/quarantine/incremental_incidents/`
  Processed-batch checkpoint     `data/state/incident_ingestion.json`

The checkpoint is local runtime state and is excluded from Git. To
replay the demo from a clean state, remove the incremental test outputs
and checkpoint before running the sample batches again. Do not delete
the normal `data/bronze/incidents/` dataset.

Merge semantics: an incoming record replaces an existing record with the
same `incident_id`; otherwise, the record is inserted. Records not
present in the incoming batch are retained.

#### Incremental processing guarantees and limitations

-   A batch already present in the checkpoint is skipped.
-   Bronze publication is performed before the batch checkpoint is
    advanced.
-   If publication fails, the checkpoint is not advanced.
-   If checkpoint saving fails after publication, the batch can be
    replayed; replay may create another physical version while
    preserving the intended business rows.
-   Incoming batch IDs are validated before being used in quarantine
    paths.
-   Quarantine, Bronze publication, and checkpoint persistence are
    separate filesystem operations. They are not a single transaction.
-   A failure while writing quarantine or while saving the checkpoint
    after quarantine has been written can require manual
    cleanup/recovery before replay, because the batch-specific
    quarantine directory already exists.
-   The implementation does not coordinate concurrent workers with locks
    or compare-and-swap. Run batches sequentially.
-   The checkpoint is a local JSON file, not a durable distributed state
    store.

### 4. Silver transformation

The Silver pipeline enriches validated Bronze records with derived
fields:

-   `incident_duration_minutes`
-   `severity_priority`
-   `is_resolved`
-   `created_date`
-   `resolution_status_consistent`

It also trims whitespace from selected text fields, including `service`
and `region`.

The Silver dataset preserves the original 22 columns and adds five
derived columns, resulting in 27 columns.

Run it with:

``` bash
python pipeline/build_silver.py
```

Silver reads the published Bronze version through the manifest-aware
reader, validates the transformed dataset, and publishes a new Silver
version only after its quality checks pass.

### 5. Silver quality gate

Before publishing Silver output, the pipeline validates:

-   Required schema and columns
-   Record-count preservation between Bronze and Silver
-   Incident-duration validity
-   Derived-field correctness
-   Lifecycle consistency between status, resolution timestamp, and
    consistency flags

A failed validator raises an error and prevents the candidate Silver
dataset from being published.

### 6. Gold analytics

The Gold layer contains two independently built analytical datasets.

#### Gold Incident Summary

**Grain:** one row per `created_date`, `service`, `region`, and
`severity`.

  -----------------------------------------------------------------------
  Column                              Description
  ----------------------------------- -----------------------------------
  `created_date`                      Date the incident was created

  `service`                           Affected service

  `region`                            Affected region

  `severity`                          Incident severity

  `incident_count`                    Total incidents in the group

  `resolved_count`                    Incidents marked resolved

  `open_count`                        Incidents marked open

  `avg_duration_minutes`              Average available incident duration
                                      in minutes
  -----------------------------------------------------------------------

Null durations are excluded from the average. If no duration is
available in a group, the average remains null rather than being
represented as zero.

Run it with:

``` bash
python pipeline/build_gold.py
```

#### Gold Resolution Performance

**Grain:** one row per `resolution_date`, `service`, and `severity`.

Only resolved incidents with a non-null resolution timestamp are
included.

  -----------------------------------------------------------------------
  Column                              Description
  ----------------------------------- -----------------------------------
  `resolution_date`                   Date the incident was resolved

  `service`                           Affected service

  `severity`                          Incident severity

  `resolved_incident_count`           Number of resolved incidents in the
                                      group

  `duration_observation_count`        Number of valid duration
                                      observations

  `avg_resolution_minutes`            Average resolution duration

  `median_resolution_minutes`         Approximate median resolution
                                      duration

  `max_resolution_minutes`            Maximum resolution duration
  -----------------------------------------------------------------------

The median uses PySpark's `percentile_approx`, so it is an approximate
percentile rather than an exact median.

Run it with:

``` bash
python pipeline/build_resolution_performance.py
```

### 7. Gold quality gates

Both Gold pipelines validate their outputs before publishing.

**Incident Summary checks:**

-   Required schema
-   Reconciliation of aggregated incident counts with Silver input
-   Non-negative resolved/open counts that sum to each group's total
-   Unique four-column grouping grain

**Resolution Performance checks:**

-   Required schema
-   Reconciliation of aggregated resolved counts with eligible Silver
    records
-   Resolution-duration metric validity
-   Unique three-column grouping grain

A failed quality check prevents the candidate Gold dataset from becoming
the published version.

### 8. Versioned dataset publishing

Silver, Gold, and the incremental Bronze path use
`src/data_publishing.py`.

Each successful publication:

1.  Generates a unique version ID from a UTC timestamp and UUID suffix.
2.  Writes candidate Parquet data to a new directory under `versions/`.
3.  Reads the staged dataset back and verifies its row count.
4.  Updates `current.json` only after verification succeeds.
5.  Allows readers to resolve the active version through the manifest.

Example layout:

``` text
data/silver/incidents/
├── current.json
└── versions/
    └── <version_id>/
        ├── part-*.parquet
        └── _SUCCESS
```

The manifest contains a version ID and a relative path such as
`versions/<version_id>`. The reader validates that the path matches the
version ID, stays within the dataset directory, and points to an
existing directory.

Failure behavior:

-   If staging or row-count verification fails, the failed candidate
    version is removed.
-   If manifest publication fails, the previous manifest remains active;
    a verified but unpublished candidate may be retained for inspection.
-   Previously published versions are retained instead of overwritten.

**Publishing limitations:** this implementation targets a single
publisher on a local filesystem. Atomic manifest replacement relies on
local filesystem semantics. Concurrent publishers are not coordinated
with locking or compare-and-swap. Cloud object storage requires
storage-specific publication and consistency guarantees.

### 9. Run-level observability

The Silver and Gold transformation pipelines use
`src/run_observability.py`. Each run creates a unique run ID and appends
a JSON Lines record.

  --------------------------------------------------------------------------------------------------
  Pipeline                            Run log
  ----------------------------------- --------------------------------------------------------------
  Silver                              `data/observability/build_silver_runs.jsonl`

  Gold Incident Summary               `data/observability/build_gold_runs.jsonl`

  Gold Resolution Performance         `data/observability/build_resolution_performance_runs.jsonl`
  --------------------------------------------------------------------------------------------------

Run records include:

-   `run_id` and `pipeline_name`
-   `status`: `RUNNING`, `SUCCESS`, or `FAILED`
-   UTC start and finish timestamps
-   Input and output record counts when available
-   Quality-gate outcome
-   Published version path after successful publication
-   Exception type and message when a run fails

Logs are local JSONL files, not a centralized logging service or
transactional audit store. Logging failures may result in a missing run
record without masking the main pipeline exception. Concurrent-writer
coordination, alerting, guaranteed persistence during filesystem
failures, and log rotation are not implemented.

## Automated Testing

Run the full test suite with:

``` bash
pytest -q
```

The suite covers schema loading, data quality, Silver and Gold
transformations, quality gates, publishing and manifest handling,
failure recovery, observability, incremental merging, checkpoint
persistence, batch skipping, invalid-record quarantine, and pipeline
contracts.

**Latest reported test result: 98 tests passed.** Re-run the suite after
making changes to confirm the current result.

## Technology Stack

-   Python 3.12
-   PySpark 4.2 / Apache Spark
-   YAML
-   Parquet
-   pytest
-   `uv` for Python environment and dependency management
-   Java 21

## Project Structure

``` text
incident-intelligence/
├── configs/
│   ├── incident_schema.yaml
│   └── incident_quality_rules.yaml
├── data/
│   ├── raw/
│   ├── incoming/
│   │   └── incidents/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   │   ├── incident_summary/
│   │   └── resolution_performance/
│   ├── quarantine/
│   ├── state/
│   └── observability/
├── pipeline/
│   ├── ingest_incidents.py
│   ├── process_incident_batch.py
│   ├── build_silver.py
│   ├── build_gold.py
│   └── build_resolution_performance.py
├── src/
│   ├── schema_loader.py
│   ├── incident_generator.py
│   ├── data_quality.py
│   ├── incremental_ingestion.py
│   ├── silver_transform.py
│   ├── silver_quality.py
│   ├── gold_transform.py
│   ├── gold_quality.py
│   ├── data_publishing.py
│   └── run_observability.py
├── tests/
│   ├── test_schema_loader.py
│   ├── test_data_quality.py
│   ├── test_silver_transform.py
│   ├── test_silver_quality.py
│   ├── test_gold_transform.py
│   ├── test_gold_quality.py
│   ├── test_gold_resolution_performance.py
│   ├── test_data_publishing.py
│   ├── test_incremental_checkpoint.py
│   ├── test_incremental_ingestion.py
│   ├── test_run_observability.py
│   └── test_pipeline_observability_contract.py
├── pyproject.toml
├── uv.lock
└── README.md
```

Generated datasets, local checkpoints, and observability logs are
development artifacts and should remain excluded from Git.

## Getting Started

### Prerequisites

-   Python 3.12
-   Java 21
-   `uv`

### Install dependencies

Run from the project root:

``` bash
uv sync
source .venv/bin/activate
```

### Run the full-load pipeline

``` bash
python pipeline/ingest_incidents.py
python pipeline/build_silver.py
python pipeline/build_gold.py
python pipeline/build_resolution_performance.py
```

Run these in order when building the full-load dataset from scratch.

### Run the incremental demonstration

Process batches sequentially:

``` bash
python pipeline/process_incident_batch.py batch_0001
python pipeline/process_incident_batch.py batch_0002
python pipeline/process_incident_batch.py batch_0003
```

Re-running a batch already recorded in
`data/state/incident_ingestion.json` should return `status: skipped`.

The incremental demonstration writes to
`data/bronze/incremental_incidents/` and does not replace the normal
full-load Bronze dataset. To reset the demonstration, first stop any
running pipeline, then remove only its incremental output and
checkpoint:

``` bash
rm -rf data/bronze/incremental_incidents
rm -rf data/quarantine/incremental_incidents
rm -f data/state/incident_ingestion.json
```

Do not remove `data/bronze/incidents/` when resetting the incremental
demonstration.

### Inspect the latest run records

``` bash
tail -n 1 data/observability/build_silver_runs.jsonl
tail -n 1 data/observability/build_gold_runs.jsonl
tail -n 1 data/observability/build_resolution_performance_runs.jsonl
```

These commands show the most recently appended record for each
transformation pipeline.

## Current Verification

The full-load dataset has been verified with:

-   100 raw incident records
-   92 valid records accepted into Bronze
-   8 invalid records quarantined
-   92 records transformed into Silver
-   27 columns in Silver
-   89 aggregated rows in Gold Incident Summary in the verified run
-   64 aggregated rows in Gold Resolution Performance in the verified
    run
-   8 columns in each Gold dataset

The incremental demonstration has also been run with sample batches:

-   After `batch_0001`: 2 published records
-   After `batch_0002`: 3 published records, including one update and
    one insert
-   After `batch_0003`: 4 published records, with one invalid row
    quarantined
-   Replaying a checkpointed batch: skipped without publishing another
    version

These values describe the current synthetic dataset and demonstrated
runs, not fixed production expectations. Gold row counts depend on the
distinct combinations of grouping dimensions. Version IDs and paths are
generated dynamically.

The latest reported regression run had **98 passing tests**.

## Engineering Principles

-   Explicit schema enforcement
-   Configurable data quality rules
-   Separation of transformation and validation logic
-   Quarantine of invalid records
-   Incremental inserts and updates
-   Checkpointed batch progress and replay handling
-   Automated regression and failure-path testing
-   Validation before publication
-   Versioned datasets and manifest-aware readers
-   Run-level observability with execution IDs and status records
-   Reproducible development workflows
-   Clear separation between local guarantees and production
    requirements
-   Explicit documentation of failure modes and operational limitations

## Roadmap

Next planned improvements:

1.  Strengthen quarantine recovery so replay after partial failure is
    safe.
2.  Add protection against concurrent batch processing.
3.  Add incremental-pipeline observability and operational metrics.
4.  Integrate the batch and transformation workflows with Apache
    Airflow.
5.  Orchestrate downstream Silver and Gold refreshes after successful
    Bronze publication.
6.  Explore downstream ML and RAG pipelines once the data platform
    orchestration is stable.
