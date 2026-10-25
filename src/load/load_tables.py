"""
Create the external table over the processed Parquet output, then validate.
"""

import argparse
import logging
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Reuse the transform stage's helpers
sys.path.insert(0, str(PROJECT_ROOT / "src" / "transform"))

from pyspark.sql import SparkSession  

from spark_job import assert_all_passed, latest_success_record, load_config, setup_logging  
from spark_session import get_spark 

logger = logging.getLogger(__name__)

TABLE = "reviews_db.reviews"
VIEWS = ["v_rating_by_app_year", "v_developer_response_rate", "v_review_length_by_rating"]
DDL_FILE = PROJECT_ROOT / "sql" / "create_tables.sql"


def render_ddl(location: Path) -> str:
    """Read DDL file and fill in the table location."""
    return DDL_FILE.read_text(encoding="utf-8").replace("{location}", location.as_uri())


def run_sql_file(spark: SparkSession, sql_text: str) -> None:
    """Run every statement in a SQL file."""
    without_comments = "\n".join(re.sub(r"--.*$", "", line) for line in sql_text.splitlines())
    for statement in without_comments.split(";"):
        statement = statement.strip()
        if statement:
            logger.info("Running: %s", " ".join(statement.split())[:80])
            spark.sql(statement)


def check_table(spark: SparkSession, location: Path, run_record: dict) -> list[dict]:
    """Compare the catalog table against the files on disk and the Stage 2 run record."""
    describe = spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}").collect()
    table_type = next((r["data_type"] for r in describe if r["col_name"] == "Type"), None)

    table_rows = spark.table(TABLE).count()
    per_platform = {r["platform"]: r["count"] for r in spark.table(TABLE).groupBy("platform").count().collect()}
    registered = spark.sql(f"SHOW PARTITIONS {TABLE}").count()
    # Partition folders look like platform=<name>/review_year=<yyyy>
    on_disk = len([p for p in location.glob("platform=*/review_year=*") if p.is_dir()])

    expected = run_record["rows"]
    expected_platforms = {"google_play": expected["google"], "app_store": expected["ios_after_dedup"]}
    return [
        {"name": "table_is_external", "passed": table_type == "EXTERNAL", "detail": f"table type {table_type}"},
        {"name": "row_count_matches_run_record", "passed": table_rows == expected["output"],
         "detail": f"table {table_rows} vs run record {expected['output']}"},
        {"name": "platform_counts_match", "passed": per_platform == expected_platforms,
         "detail": f"table {per_platform} vs run record {expected_platforms}"},
        {"name": "all_partitions_registered", "passed": registered == on_disk,
         "detail": f"{registered} registered vs {on_disk} folders on disk"},
    ]


def log_pruning_plan(spark: SparkSession) -> None:
    """Filter on the partition columns is applied to the folders, not the rows."""
    plan = spark.sql(
        f"EXPLAIN FORMATTED SELECT COUNT(*) FROM {TABLE} WHERE platform = 'app_store' AND review_year = 2019"
    ).collect()[0][0]
    for line in plan.splitlines():
        if "PartitionFilters" in line or "Location" in line:
            logger.info("Plan: %s", line.strip())


def show_view_samples(spark: SparkSession) -> None:
    """Print first rows of each view."""
    for view in VIEWS:
        print(f"\n-- reviews_db.{view} --")
        spark.sql(f"SELECT * FROM reviews_db.{view} ORDER BY 1, 2, 3 LIMIT 10").show(truncate=False)


def demo_drop_and_recreate(spark: SparkSession, ddl: str, location: Path, run_record: dict) -> None:
    """Drop the table, show the Parquet files survive, then recreate it from the same files."""
    spark.sql(f"DROP TABLE {TABLE}")
    remaining = len(list(location.rglob("*.parquet")))
    if remaining == 0:
        raise RuntimeError("The Parquet files were deleted: the table was managed, not external")
    logger.info("Dropped %s. %d Parquet files are still on disk (external table).", TABLE, remaining)

    run_sql_file(spark, ddl)   # the DDL is idempotent, so this recreates everything
    assert_all_passed(check_table(spark, location, run_record))
    logger.info("Recreated %s from the same files", TABLE)


def run(spark: SparkSession, config: dict, demo_drop: bool) -> None:
    paths = config["paths"]
    location = paths["processed_base"] / "reviews"
    if not any(location.rglob("*.parquet")):
        raise FileNotFoundError(f"No Parquet files under {location}. Run the Spark job first.")

    run_record = latest_success_record(paths["run_records_dir"])
    if run_record is None:
        raise RuntimeError("No successful Stage 2 run record found. Run the Spark job first.")

    ddl = render_ddl(location)
    run_sql_file(spark, ddl)
    assert_all_passed(check_table(spark, location, run_record))
    log_pruning_plan(spark)
    show_view_samples(spark)

    if demo_drop:
        demo_drop_and_recreate(spark, ddl, location, run_record)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the external table over the processed Parquet output.")
    parser.add_argument("--demo-drop", action="store_true",
                        help="drop the table, show the files survive, then recreate it")
    args = parser.parse_args()

    config = load_config()
    setup_logging(config["paths"]["log_dir"], config["settings"]["log_level"], name="load")

    spark = None
    try:
        if "catalog_dir" not in config["paths"]:
            raise ValueError("Missing paths.catalog_dir in config.yaml")
        spark = get_spark("load_tables", catalog_dir=config["paths"]["catalog_dir"])
        run(spark, config, args.demo_drop)
    except Exception:
        logger.exception("Load stage failed")
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()