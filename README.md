# App Store Reviews Pipeline

A data engineering project that ingests app review data from Kaggle, processes it with Spark, and loads it into external tables.

| Stage | Status |
|---|---|
| 1. Ingest: download data from APIs | Done |
| 2. Process: Spark jobs | Planned |
| 3. Load: external tables | Planned |

## Datasets

| Name | Kaggle ID | Format |
|---|---|---|
| `google_reviews` | `hassaanmustafavi/google-play-app-reviews-dataset` | CSV |
| `ios_dataset` | `crawlfeeds/app-store-reviews-dataset` | JSON |

## Stage 1: Ingest

`src/ingest/download_kaggle.py` downloads each dataset listed in `config/config.yaml`, keeps a snapshot only when the data has changed, and records every run.

### How it works

1. Download the dataset into `data/staging/<name>/` (never directly into `raw`).
2. Compute a SHA-256 hash for each extracted file.
3. Compare against the last stored snapshot's hashes.
4. **Changed or first run:** move the staged files into `data/raw/<name>/<UTC timestamp>/` and write a `new` metadata record.
5. **Unchanged:** discard the staged files and write an `unchanged` metadata record pointing to the matching snapshot.

### Design decisions

- **Staging, then atomic rename.** A crash mid-download can never leave a half-written folder in `raw`.
- **Move data first, then write the record.** The metadata record is the source of truth. A snapshot folder without a record is treated as uncommitted.
- **Checksums over timestamps.** Hashing file contents detects real changes. Re-downloading identical data never creates a duplicate snapshot.
- **Extracted files are hashed, not the zip.** Zip files embed timestamps and compression details, so identical data can produce different zip hashes.
- **Immutable snapshots.** Nothing in `raw` is ever overwritten. Each observed version of the data is kept, and a second snapshot on the same day is possible if the data changes between runs.
- **Failures are isolated and visible.** One dataset failing does not stop the others. The run summary reports it, and the script exits with code 1.

Wiki for Deployment Documentation
- https://github.com/CarlosC21/appstore-reviews-pipeline/wiki
