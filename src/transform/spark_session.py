"""Create the SparkSession used by every Spark script in this project."""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

MASTER = "local[4]" 

def _prepare_windows() -> None:
    """Make Hadoop's native helpers available on Windows."""
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        raise RuntimeError(
            "HADOOP_HOME is not set. Point it at the folder that contains bin\\winutils.exe "
            "and bin\\hadoop.dll (see the runbook)."
        )
    # Java finds hadoop.dll through PATH, so put it first instead of relying on the user's PATH
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]


def _use_persistent_catalog(builder: SparkSession.Builder, catalog_dir: Path) -> SparkSession.Builder:
    """Point the session at a Hive metastore (embedded Derby) stored in catalog_dir."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    metastore = (catalog_dir / "metastore_db").as_posix()
    return (
        builder
        .config("spark.sql.warehouse.dir", (catalog_dir / "warehouse").as_uri())
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore};create=true")
        # A scratch folder Hive creates itself avoids needing a pre-made C:\tmp\hive on Windows
        .config("spark.hadoop.hive.exec.scratchdir", (catalog_dir / "scratch").as_posix())
        .enableHiveSupport()
    )


def get_spark(app_name: str, catalog_dir: Path | None = None) -> SparkSession:
    """Return a local SparkSession configured for this project."""
    # Spark's Python workers must use the same interpreter as this script.
    # On Windows the default name, "python3", does not exist.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if os.name == "nt":
        _prepare_windows()

    builder = (
        SparkSession.builder
        .master(MASTER)
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if catalog_dir is not None:
        builder = _use_persistent_catalog(builder, Path(catalog_dir))
    return builder.getOrCreate()