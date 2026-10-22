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

<img width="679" height="741" alt="image" src="https://github.com/user-attachments/assets/2f76be76-1102-47c2-8828-ca0e5166f8bf" />
<img width="669" height="352" alt="image" src="https://github.com/user-attachments/assets/80a9bb5b-7abe-42c8-b7d3-9b8cd87dbb6d" />
<img width="670" height="114" alt="image" src="https://github.com/user-attachments/assets/bbef96e1-1bb0-41df-baba-e653250f4a69" />
