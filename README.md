# App Store Reviews Pipeline

A data engineering project that ingests app review data from Kaggle, processes it with Spark, and loads it into external tables.

<img width="616" height="179" alt="image" src="https://github.com/user-attachments/assets/9f55c00d-f7a3-4be0-a50f-7919940fad2d" />

## Datasets

<img width="680" height="107" alt="image" src="https://github.com/user-attachments/assets/2c181035-a816-4eaf-afac-010f31ba2af8" />

## Pipeline

**Stage 1: Ingest.** Downloads each dataset through the Kaggle API into a staging folder, hashes the files, and commits a new snapshot to `data/raw/` only when the content changed. Every run writes a metadata record.

**Stage 2: Process.** A Spark job reads the latest raw snapshots with explicit schemas, cleans and unions both sources into one review table, runs six quality checks, and writes partitioned Parquet to `data/processed/reviews/`. Result: 161 Google Play + 10,585 App Store reviews (3 duplicates removed) = **10,746 rows**.

<img width="1865" height="472" alt="image" src="https://github.com/user-attachments/assets/d5dcffba-16c8-47e0-8338-cddc424854a6" />

**Stage 3: Load.** Planned: external tables over the Parquet output.

### Highlights

- **Idempotent.** Reruns never duplicate data. Ingest compares content checksums, and the Spark job skips snapshots it has already processed.
- **Crash-safe.** Both stages write to a staging area and publish with a rename, so a failure never leaves a half-written table.
- **Quality gate.** A failed check stops the job before anything is published and returns exit code 1.
- **Traceable.** Every output row records the raw snapshot it came from, and every run leaves a metadata or run record.

## Run it

```
pip install -r requirements.txt
python src/ingest/download_kaggle.py
python src/transform/spark_job.py
```

Credentials, Java and Windows setup, configuration, and troubleshooting are in the [Runbook](https://github.com/CarlosC21/appstore-reviews-pipeline/wiki/Runbook). How each stage works and why it is designed this way is in [Design & Decisions](https://github.com/CarlosC21/appstore-reviews-pipeline/wiki/Design-Decisions).

## Future improvements

- Retry with exponential backoff for transient network errors in ingest
- Lock file to prevent overlapping runs
- Shared module for config, logging, and atomic writes (currently duplicated between stages)
- Scheduled daily runs with alerting on a non-zero exit code
