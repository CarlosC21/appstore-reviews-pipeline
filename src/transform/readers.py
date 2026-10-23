"""Locate the latest committed raw snapshots and read them with explicit schemas."""

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from schemas import GOOGLE_SCHEMA, IOS_SCHEMA

PROJECT_ROOT = Path(__file__).resolve().parents[2]
METADATA_DIR = PROJECT_ROOT / "data" / "metadata"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

GOOGLE_FILE = "GooglePlay_App_Data.csv"
IOS_FILE = "app_store_reviews_dataset.json"


def latest_snapshot_dir(dataset_name: str) -> Path:
    """Resolve the newest committed snapshot folder through the ingest metadata records."""
    for path in sorted((METADATA_DIR / dataset_name).glob("*.json"), reverse=True):
        with open(path, "r", encoding="utf-8") as f:
            record = json.load(f)
        if record.get("status") == "new":
            return RAW_DIR / record["snapshot_dir"]
    raise FileNotFoundError(f"No committed snapshot found for {dataset_name}")


def read_google(spark: SparkSession, path: Path) -> DataFrame:
    """Read the Google Play CSV with the explicit schema."""
    return (
        spark.read
        .schema(GOOGLE_SCHEMA)
        .option("header", True)           # the first line is the header row
        .option("enforceSchema", False)   # check header names against the schema instead of ignoring them
        .option("multiLine", True)        # a quoted review may contain line breaks
        .option("quote", '"')
        .option("escape", '"')            # a doubled quote inside a quoted field is a literal quote
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")       # a malformed row stops the job instead of silently becoming nulls
        .csv(str(path))
    )


def read_ios(spark: SparkSession, path: Path) -> DataFrame:
    """Read the App Store JSON (a single array of objects) with the explicit schema."""
    return (
        spark.read
        .schema(IOS_SCHEMA)
        .option("multiLine", True)        # the file is one JSON array, not one object per line
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
        .json(str(path))
    )