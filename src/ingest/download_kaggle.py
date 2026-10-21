import hashlib
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_config(config_path: Path) -> dict:
    """Read config.yaml and return it as a dict, with paths made absolute."""
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    for key, value in config["paths"].items():
        config["paths"][key] = PROJECT_ROOT / value

    return config


def setup_logging(log_dir: Path, level: str) -> None:
    """Send log messages to the console and to a daily UTC log file."""
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"ingest_{datetime.now(timezone.utc).date().isoformat()}.log"
    log_level = getattr(logging, level.upper())

    logging.Formatter.converter = time.gmtime   # format all timestamps in UTC
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
def get_kaggle_api():
    """Create and authenticate the Kaggle API client once per run."""
    # Imported here because the kaggle package can authenticate at import time
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    return api
def download_dataset(api, kaggle_id: str, staging_dir: Path) -> list[Path]:
    """Download and unzip one dataset into a clean staging folder.

    Returns every file downloaded, including files in subfolders.
    Raises on any failure; the caller is responsible for logging it.
    """
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading %s into %s", kaggle_id, staging_dir)
    api.dataset_download_files(kaggle_id, path=str(staging_dir), unzip=True)

    files = sorted(p for p in staging_dir.rglob("*") if p.is_file())
    if not files:
        raise RuntimeError(f"Download of {kaggle_id} produced no files")
    return files

def hash_file(path: Path, algorithm: str = "sha256", chunk_size: int = 1024 * 1024) -> str:
    """Return the hex digest of one file, reading it in chunks."""
    hasher = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()

def hash_files(files: list[Path], base_dir: Path, algorithm: str) -> dict[str, dict]:
    """Return {relative_path: {"size_bytes": ..., "hash": ...}} for each file."""
    result = {}
    for path in sorted(files):
        key = path.relative_to(base_dir).as_posix()
        result[key] = {
            "size_bytes": path.stat().st_size,
            "hash": hash_file(path, algorithm),
        }
    return result

def utc_timestamp() -> str:
    """UTC time as a string that is safe in Windows file names and sorts chronologically."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def write_metadata(metadata_dir: Path, record: dict) -> Path:
    """Write one run record to metadata_dir/<dataset>/<run_timestamp>.json, atomically."""
    dataset_dir = metadata_dir / record["dataset"]
    dataset_dir.mkdir(parents=True, exist_ok=True)

    final_path = dataset_dir / f"{record['run_timestamp']}.json"
    tmp_path = final_path.with_suffix(".json.tmp")

    # Write to a temp file first; a crash here leaves only a .tmp file behind
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())

    # Atomic: readers see either no record or the complete record, never half of one
    os.replace(tmp_path, final_path)
    return final_path


def latest_snapshot(metadata_dir: Path, dataset_name: str) -> dict | None:
    """Return the most recent record with status 'new', or None if there isn't one."""
    dataset_dir = metadata_dir / dataset_name
    if not dataset_dir.exists():
        return None

    # Timestamps sort chronologically as plain text, so reverse order = newest first
    for path in sorted(dataset_dir.glob("*.json"), reverse=True):
        try:
            with open(path, "r", encoding="utf-8") as f:
                record = json.load(f)
        except json.JSONDecodeError:
            logger.warning("Skipping unreadable metadata file: %s", path)
            continue
        if record.get("status") == "new":
            return record
    return None

def commit_snapshot(staging_dir: Path, raw_base: Path, dataset_name: str, timestamp: str) -> Path:
    """Atomically move a staged download into the raw layer. Returns the new snapshot path."""
    destination = raw_base / dataset_name / timestamp
    destination.parent.mkdir(parents=True, exist_ok=True)

    if staging_dir.stat().st_dev != destination.parent.stat().st_dev:
        raise RuntimeError("staging_dir and raw_base must be on the same volume for an atomic move")

    # Same-volume rename: observers see no folder or the complete folder, never half
    os.replace(staging_dir, destination)
    return destination

def is_unchanged(files: dict, algorithm: str, previous: dict | None) -> bool:
    """True if the new download is identical to the last stored snapshot."""
    if previous is None:
        return False
    if previous.get("algorithm") != algorithm:
        return False
    return files == previous["files"]


def process_dataset(api, dataset: dict, config: dict) -> str:
    """Run the full ingest for one dataset. Returns 'new' or 'unchanged'."""
    name = dataset["name"]
    paths = config["paths"]
    algorithm = config["settings"]["checksum_algorithm"]
    staging = paths["staging_dir"] / name
    timestamp = utc_timestamp()

    downloaded = download_dataset(api, dataset["kaggle_id"], staging)
    files = hash_files(downloaded, staging, algorithm)
    previous = latest_snapshot(paths["metadata_dir"], name)

    record = {
        "dataset": name,
        "kaggle_id": dataset["kaggle_id"],
        "run_timestamp": timestamp,
        "algorithm": algorithm,
        "files": files,
    }

    if is_unchanged(files, algorithm, previous):
        shutil.rmtree(staging)
        record.update(status="unchanged", snapshot_dir=None,
                      matches_snapshot=previous["snapshot_dir"])
        write_metadata(paths["metadata_dir"], record)
        logger.info("%s: unchanged since %s", name, previous["run_timestamp"])
        return "unchanged"

    # Commit order: move the data first, then write the record (the record is the source of truth)
    snapshot = commit_snapshot(staging, paths["raw_base"], name, timestamp)
    record.update(status="new",
                  snapshot_dir=snapshot.relative_to(paths["raw_base"]).as_posix(),
                  matches_snapshot=None)
    write_metadata(paths["metadata_dir"], record)
    logger.info("%s: new snapshot committed at %s", name, snapshot)
    return "new"

def main() -> None:
    config = load_config(PROJECT_ROOT / "config" / "config.yaml")
    setup_logging(config["paths"]["log_dir"], config["settings"]["log_level"])

    for key in ("raw_base", "staging_dir", "metadata_dir"):
        config["paths"][key].mkdir(parents=True, exist_ok=True)

    results = {}
    try:
        api = get_kaggle_api()
    except Exception:
        logger.exception("Kaggle authentication failed")
        sys.exit(1)
    for dataset in config["datasets"]:
        try:
            results[dataset["name"]] = process_dataset(api, dataset, config)
        except Exception:
            # Single place where failures are logged, so each traceback appears once
            logger.exception("Ingest failed for %s", dataset["name"])
            results[dataset["name"]] = "FAILED"

    logger.info("Run summary: %s", results)
    if "FAILED" in results.values():
        sys.exit(1)

if __name__ == "__main__":
    main()