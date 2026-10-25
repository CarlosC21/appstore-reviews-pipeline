"""
Read the latest raw snapshots, clean and combine them, check quality, write Parquet.
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cleaning import (
    clean_google,
    clean_ios,
    combine,
    conform_google,
    conform_ios,
    total_parse_failures,
)
from readers import GOOGLE_FILE, IOS_FILE, PROJECT_ROOT, latest_snapshot_dir, read_google, read_ios
from spark_session import get_spark

logger = logging.getLogger(__name__)

# Dataset names used in config.yaml
GOOGLE_DATASET = "google_reviews"
IOS_DATASET = "ios_dataset"


# --- Config and logging ------------------------------------------------------------------------

def load_config() -> dict:
    """Read config.yaml and make every path absolute."""
    with open(PROJECT_ROOT / "config" / "config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    config["paths"] = {key: PROJECT_ROOT / value for key, value in config["paths"].items()}
    return config


def validate_config(config: dict) -> None:
    """Fail early, with a clear message, if a setting this job needs is missing."""
    transform = config.get("transform") or {}
    app_name = transform.get("google_app_name", "")
    if not app_name or app_name.upper().startswith("TODO") or app_name.startswith("<"):
        raise ValueError("Set transform.google_app_name in config.yaml (the Google file does not name its app)")
    if not transform.get("partition_columns"):
        raise ValueError("Set transform.partition_columns in config.yaml")
    for key in ("processed_base", "run_records_dir"):
        if key not in config["paths"]:
            raise ValueError(f"Missing paths.{key} in config.yaml")


def setup_logging(log_dir: Path, level: str, name: str = "transform") -> None:
    """Log to the console and to a daily UTC log file named <name>_<date>.log."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{name}_{datetime.now(timezone.utc).date().isoformat()}.log"
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
    )


def utc_timestamp() -> str:
    """UTC time that is safe in Windows file names and sorts chronologically."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


# --- Data quality checks -----------------------------------------------------------------------

class DataQualityError(Exception):
    """Raised when one or more quality checks fail."""


def _result(name: str, passed: bool, detail: str) -> dict:
    return {"name": name, "passed": bool(passed), "detail": detail}


def run_checks(df: DataFrame, google_rows: int, ios_rows: int, parse_failures: int) -> list[dict]:
    """Run every check and return the results. Nothing is raised here."""
    stats = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("platform").isNull().cast("int")).alias("null_platform"),
        F.sum(F.col("review_id").isNull().cast("int")).alias("null_review_id"),
        F.sum(F.col("rating").isNull().cast("int")).alias("null_rating"),
        F.sum(F.col("review_ts").isNull().cast("int")).alias("null_review_ts"),
        F.sum(((F.col("rating") < 1) | (F.col("rating") > 5)).cast("int")).alias("bad_rating"),
        F.sum((F.col("review_ts") > F.col("processed_at")).cast("int")).alias("future_reviews"),
        F.countDistinct("platform", "review_id").alias("distinct_keys"),
    ).first()

    rows = stats["rows"]
    null_counts = {
        "platform": stats["null_platform"] or 0,
        "review_id": stats["null_review_id"] or 0,
        "rating": stats["null_rating"] or 0,
        "review_ts": stats["null_review_ts"] or 0,
    }
    bad_rating = stats["bad_rating"] or 0
    future_reviews = stats["future_reviews"] or 0

    return [
        _result("row_reconciliation", rows == google_rows + ios_rows,
                f"output {rows} vs inputs {google_rows} + {ios_rows}"),
        _result("no_null_keys_or_required_fields", sum(null_counts.values()) == 0,
                f"null counts {null_counts}"),
        _result("rating_between_1_and_5", bad_rating == 0, f"{bad_rating} rows out of range"),
        _result("unique_platform_review_id", stats["distinct_keys"] == rows,
                f"{stats['distinct_keys']} distinct keys for {rows} rows"),
        _result("no_future_review_dates", future_reviews == 0, f"{future_reviews} rows after processed_at"),
        _result("no_timestamp_parse_failures", parse_failures == 0, f"{parse_failures} failures"),
    ]


def assert_all_passed(results: list[dict]) -> None:
    """Log every result, then raise if any check failed."""
    for r in results:
        level = logging.INFO if r["passed"] else logging.ERROR
        logger.log(level, "[%s] %s: %s", "PASS" if r["passed"] else "FAIL", r["name"], r["detail"])

    failed = [r["name"] for r in results if not r["passed"]]
    if failed:
        raise DataQualityError(f"Quality checks failed: {failed}")


# --- Output, publishing, and run records -------------------------------------------------------

def write_parquet(df: DataFrame, staging_dir: Path, partition_columns: list[str]) -> None:
    """Write partitioned Parquet into a clean staging folder."""
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    (
        df.repartition(*partition_columns)    
        .write.mode("overwrite")
        .partitionBy(*partition_columns)
        .parquet(str(staging_dir))           
    )


def publish(staging_dir: Path, final_dir: Path) -> None:
    """Swap the verified staging output into place. Both must be on the same volume."""
    if not (staging_dir / "_SUCCESS").exists():
        raise RuntimeError("Spark did not finish writing: no _SUCCESS marker in staging")

    previous_dir = final_dir.with_name(final_dir.name + "_previous")
    if previous_dir.exists():
        shutil.rmtree(previous_dir)
    if final_dir.exists():
        os.rename(final_dir, previous_dir)
    os.rename(staging_dir, final_dir)
    if previous_dir.exists():
        shutil.rmtree(previous_dir)


def write_and_publish(
    spark: SparkSession,
    df: DataFrame,
    staging_dir: Path,
    final_dir: Path,
    partition_columns: list[str],
    expected_rows: int,
) -> None:
    """Write Parquet to staging, verify it by reading it back, then publish it."""
    write_parquet(df, staging_dir, partition_columns)
    written_rows = spark.read.parquet(str(staging_dir)).count()
    if written_rows != expected_rows:
        raise RuntimeError(f"Read-back mismatch: wrote {expected_rows} rows, read {written_rows}")
    publish(staging_dir, final_dir)


def write_run_record(run_dir: Path, record: dict) -> Path:
    """Write one run record atomically"""
    run_dir.mkdir(parents=True, exist_ok=True)
    final_path = run_dir / f"{record['run_timestamp']}.json"
    tmp_path = final_path.with_suffix(".json.tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, final_path)
    return final_path


def latest_success_record(run_dir: Path) -> dict | None:
    """Return the newest successful run record, or None."""
    if not run_dir.exists():
        return None
    for path in sorted(run_dir.glob("*.json"), reverse=True):
        with open(path, "r", encoding="utf-8") as f:
            record = json.load(f)
        if record.get("status") == "success":
            return record
    return None


# --- The job -----------------------------------------------------------------------------------

def build_table(
    spark: SparkSession,
    google_dir: Path,
    ios_dir: Path,
    app_name: str,
    processed_at: datetime,
) -> tuple[DataFrame, dict]:
    """Read, clean, conform, and combine both sources."""
    raw_google = read_google(spark, google_dir / GOOGLE_FILE)
    raw_ios = read_ios(spark, ios_dir / IOS_FILE)
    parse_failures = total_parse_failures(raw_google, raw_ios)

    google = clean_google(raw_google)
    ios = clean_ios(raw_ios)
    counts = {
        "google": google.count(),
        "ios_raw": raw_ios.count(),
        "ios_after_dedup": ios.count(),
        "parse_failures": parse_failures,
    }

    combined = combine(
        conform_google(google, app_name, google_dir.name),
        conform_ios(ios, ios_dir.name),
        processed_at,
    )
    return combined, counts


def run(spark: SparkSession, config: dict, force: bool = False) -> None:
    """Resolve inputs, skip if already processed, build and check the table, publish it, record the run."""
    paths = config["paths"]
    app_name = config["transform"]["google_app_name"]
    partition_columns = config["transform"]["partition_columns"]
    final_dir = paths["processed_base"] / "reviews"
    staging_dir = paths["processed_base"] / "_staging" / "reviews"

    google_dir = latest_snapshot_dir(GOOGLE_DATASET)
    ios_dir = latest_snapshot_dir(IOS_DATASET)
    inputs = {GOOGLE_DATASET: google_dir.name, IOS_DATASET: ios_dir.name}

    # Idempotency: the output is a pure function of the input snapshots
    previous = latest_success_record(paths["run_records_dir"])
    if not force and previous and previous["inputs"] == inputs and final_dir.exists():
        logger.info("Inputs unchanged since run %s: nothing to do", previous["run_timestamp"])
        return

    run_timestamp = utc_timestamp()
    processed_at = datetime.now(timezone.utc)
    logger.info("Processing snapshots %s", inputs)

    combined, counts = build_table(spark, google_dir, ios_dir, app_name, processed_at)
    duplicates_removed = counts["ios_raw"] - counts["ios_after_dedup"]
    logger.info(
        "Rows in: google=%d, app_store=%d (%d duplicates removed)",
        counts["google"], counts["ios_after_dedup"], duplicates_removed,
    )

    combined = combined.cache()
    try:
        output_rows = combined.count()
        results = run_checks(combined, counts["google"], counts["ios_after_dedup"], counts["parse_failures"])
        assert_all_passed(results)
        write_and_publish(spark, combined, staging_dir, final_dir, partition_columns, output_rows)
    finally:
        combined.unpersist()


    record = {
        "run_timestamp": run_timestamp,
        "status": "success",
        "inputs": inputs,
        "rows": {
            "google": counts["google"],
            "ios_raw": counts["ios_raw"],
            "ios_after_dedup": counts["ios_after_dedup"],
            "ios_duplicates_removed": duplicates_removed,
            "output": output_rows,
        },
        "quality_checks": results,
        "output_path": final_dir.relative_to(PROJECT_ROOT).as_posix(),
        "partition_columns": partition_columns,
        "format": "parquet (snappy)",
        "spark_version": spark.version,
    }
    write_run_record(paths["run_records_dir"], record)
    logger.info("Published %d rows to %s", output_rows, final_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Process the latest raw snapshots into partitioned Parquet.")
    parser.add_argument("--force", action="store_true",
                        help="reprocess even if these snapshots were already processed")
    args = parser.parse_args()

    config = load_config()
    setup_logging(config["paths"]["log_dir"], config["settings"]["log_level"])

    spark = None
    try:
        validate_config(config)
        spark = get_spark("reviews_job")
        run(spark, config, force=args.force)
    except Exception:
        logger.exception("Transform job failed")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()