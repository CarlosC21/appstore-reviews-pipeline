# App Store Reviews Pipeline

A data engineering project that ingests app review data from Kaggle, processes it with Spark, and loads it into external tables.

<img width="616" height="179" alt="image" src="https://github.com/user-attachments/assets/9f55c00d-f7a3-4be0-a50f-7919940fad2d" />


## Datasets

<img width="680" height="107" alt="image" src="https://github.com/user-attachments/assets/2c181035-a816-4eaf-afac-010f31ba2af8" />


## Stage 1: Ingest

`src/ingest/download_kaggle.py` downloads each dataset listed in `config/config.yaml` through the Kaggle API, stores a snapshot only when the data has changed, and writes a metadata record for every run.

### How it works

1. Authenticate with the Kaggle API once per run.
2. Download each dataset into `data/staging/<name>/` (never directly into `raw`).
3. Compute a SHA-256 hash for every extracted file.
4. Compare against the last stored snapshot.
5. **Changed or first run:** move the staged files into `data/raw/<name>/<UTC timestamp>/`, then write a `new` metadata record.
6. **Unchanged:** discard the staged files and write an `unchanged` metadata record that points to the matching snapshot.

### Quick start

Requires Python 3.10+ and a Kaggle API token (see the Runbook for credential setup).

```
pip install -r requirements.txt
python src/ingest/download_kaggle.py
```

A successful run ends with a summary such as `{'google_reviews': 'new', 'ios_dataset': 'new'}`. Rerunning with unchanged source data reports `unchanged` and creates no new snapshot. The script exits with code 1 if any dataset fails.

Setup, configuration, output layout, and troubleshooting are in the Runbook - 
- https://github.com/CarlosC21/appstore-reviews-pipeline/wiki/Runbook

### Design decisions

- **Staging, then atomic rename.** A crash mid-download never leaves a half-written folder in `raw`.
- **Move data first, then write the record.** The metadata record is the source of truth. A snapshot folder without a record is treated as uncommitted.
- **Checksums over timestamps.** Hashing file contents detects real changes, so re-downloading identical data never creates a duplicate snapshot.
- **Extracted files are hashed, not the zip.** Zip files embed timestamps and compression details, so identical data can produce different zip hashes.
- **Files tracked by relative path.** Nested datasets are supported, and same-named files in different folders do not collide.
- **Immutable snapshots.** Nothing in `raw` is ever overwritten. Each observed version is kept, and a second snapshot on the same day is possible if the data changes.
- **Failures are isolated and logged once.** One dataset failing does not stop the others. Each traceback is logged at a single point, and the run summary reports every result.
- **UTC everywhere.** Snapshot folders, metadata records, and log files all use UTC, so timestamps line up across artifacts.
- **Config over code.** Datasets, paths, hash algorithm, and log level live in `config.yaml`. Adding a dataset needs no code change.

## Stage 2: Process Data using Spark Jobs

<img width="1865" height="472" alt="image" src="https://github.com/user-attachments/assets/d5dcffba-16c8-47e0-8338-cddc424854a6" />

`src/transform/spark_job.py` reads the latest committed raw snapshots, cleans and combines them, runs quality checks, and writes one partitioned Parquet table to `data/processed/reviews/`.

**Result:** 161 Google Play reviews + 10,585 App Store reviews (3 exact duplicates removed) = **10,746 rows**.

### Data Quality Issues
<img width="817" height="296" alt="image" src="https://github.com/user-attachments/assets/5a44b5ee-30ad-48d1-899d-b8abbe093586" />

### Model Decision
Nothing to join, so the job **unions** both sources into one review table with a `platform` column. The key is `(platform, review_id)`. Fields that exist on one side only (`thumbs_up`, `app_version`, `is_edited`, `app_id`) stay null on the other side.

### How it works

1. Resolve the latest snapshots through the ingest metadata (never a hardcoded folder). Skip if these snapshots were already processed.
2. Read both sources with explicit schemas. Malformed rows stop the job (`FAILFAST`).
3. Clean each source: rename to snake_case, parse timestamps, cast types, flatten the nested developer response, and deduplicate App Store reviews on `_id`.
4. Conform and union into one table, adding derived columns (`review_date`, `review_year`, `review_length`, `has_developer_response`) and lineage columns (`source_snapshot`, `processed_at`).
5. Run six quality checks. Any failure stops the job before anything is written.
6. Write Parquet to `data/processed/_staging/`, read it back to verify the row count, then swap it into `data/processed/reviews/`.
7. Write a run record to `data/processing_runs/`.

**Quality checks:** row counts reconcile, no nulls in key fields, rating between 1 and 5, `(platform, review_id)` unique, no future review dates, zero timestamp parse failures.

### Quick start

Requires a JDK (Java 17+ for Spark 4) and the packages in `requirements.txt`. On Windows, set `HADOOP_HOME` to a folder containing `bin\winutils.exe` and `bin\hadoop.dll` (see the Runbook).

```
python src/transform/spark_job.py            # process the latest snapshots, skip if already done
python src/transform/spark_job.py --force    # reprocess even if already done
```

The job exits with code 0 on success or skip, and 1 on any failure. Settings live in `config/config.yaml`: `spark.master` (for example `local[4]`), `spark.shuffle_partitions`, `transform.google_app_name`, and `transform.partition_columns`.

### Output

```
data/processed/reviews/platform=<name>/review_year=<yyyy>/part-*.snappy.parquet
data/processing_runs/<UTC timestamp>.json
```

22 partition folders with one file each. The run record holds the input snapshots, row counts, and every quality check result.

### Design decisions

- **Explicit schemas.** They act as a contract with the source and avoid an inference pass. CSV schemas apply by position, so `enforceSchema=False` makes Spark validate the header instead of silently shifting columns.
- **Parse failures are counted, not hidden.** `try_to_timestamp` turns a bad value into null (Spark 4 runs in ANSI mode, where `to_timestamp` raises), and a quality check requires zero failures.
- **Timestamps are assumed UTC.** Three of the four source timestamp columns carry no timezone. The session timezone is set to UTC in one place. The fourth states `Z` and is parsed from the data.
- **Union by name.** `unionByName(allowMissingColumns=True)` matches columns by name, and a fixed column list sets the output order.
- **Quality gate before the write.** Checks run in a single aggregation, so Spark scans the data once. A failure publishes nothing.
- **Staging, verify, swap.** A crash or a bad write never reaches the published table. The run record is written last.
- **Idempotent.** The output is a pure function of the input snapshots, so rerunning is safe and the run record lets the job skip repeat work.
- **Partitioned by `platform` and `review_year`.** `repartition` before `partitionBy` gives one file per folder. At about 10,700 rows partitioning is not needed for performance. It demonstrates the pattern, and at this size it produces small files.
- **Core count is configuration.** `local[*]` ties behavior to the host machine, so the master is set in config (default two threads). On a cluster it would be passed at `spark-submit`, not set in code.

## Future improvements

- Retry with exponential backoff for transient network errors in ingest
- Lock file to prevent overlapping runs
- Shared module for config loading, logging, and atomic file writes (currently duplicated between ingest and transform)
- Schema drift check: the explicit JSON schema silently ignores new source fields
- Scheduled daily runs with alerting on a non-zero exit code
