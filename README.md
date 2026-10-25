# App Store Reviews Pipeline

A data engineering project that ingests app review data from Kaggle, processes it with Spark, and loads it into external tables.

<img width="501" height="143" alt="image" src="https://github.com/user-attachments/assets/a391b9fb-b3bd-409d-8e1c-d5d4e8f02228" />


## Datasets

<img width="680" height="107" alt="image" src="https://github.com/user-attachments/assets/2c181035-a816-4eaf-afac-010f31ba2af8" />

## Pipeline

**Stage 1: Ingest.** Downloads each dataset through the Kaggle API into a staging folder, hashes the files, and commits a new snapshot to `data/raw/` only when the content changed. Every run writes a metadata record.

**Stage 2: Process.** A Spark job reads the latest raw snapshots with explicit schemas, cleans and unions both sources into one review table, runs six quality checks, and writes partitioned Parquet to `data/processed/reviews/`. Result: 161 Google Play + 10,585 App Store reviews (3 duplicates removed) = **10,746 rows**.

<img width="1865" height="472" alt="image" src="https://github.com/user-attachments/assets/d5dcffba-16c8-47e0-8338-cddc424854a6" />

**Stage 3: Load.** Defines an external Parquet table, `reviews_db.reviews`, over the Stage 2 output in a Hive metastore, partitioned by `platform` and `review_year`. It checks the table against the Stage 2 run record (table type, row counts, registered partitions) and creates three views for the business questions. Dropping the table leaves the 22 Parquet files in place.

### Highlights

- **Idempotent.** Reruns never duplicate data. Ingest compares content checksums, the Spark job skips snapshots it has already processed, and the table DDL can be rerun safely.
- **Crash-safe.** Ingest and the Spark job write to a staging area and publish with a rename, so a failure never leaves a half-written table.
- **Quality gate.** A failed check stops the job before anything is published and returns exit code 1.
- **External by design.** The table is a definition over files the pipeline owns, and it is validated against the pipeline's own run records.
- **Traceable.** Every output row records the raw snapshot it came from, and every run leaves a metadata or run record.

## Run it

```
pip install -r requirements.txt
python src/ingest/download_kaggle.py
python src/transform/spark_job.py
python src/load/load_tables.py
```

Credentials, Java and Windows setup, configuration, and troubleshooting are in the [Runbook](https://github.com/CarlosC21/appstore-reviews-pipeline/wiki/Runbook). How each stage works and why it is designed this way is in [Design & Decisions](https://github.com/CarlosC21/appstore-reviews-pipeline/wiki/Design-&-Decisions).
