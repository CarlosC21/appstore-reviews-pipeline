"""Create the SparkSession used by every Spark script in this project."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Return a local SparkSession configured for this project."""
    # Spark's Python workers must use the same interpreter as this script
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # On Windows, Hadoop's local file writes need winutils.exe and hadoop.dll

    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            raise RuntimeError(
                "HADOOP_HOME is not set. Point it at the folder that contains bin\\winutils.exe "
                "and bin\\hadoop.dll (see the runbook)."
            )
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]

    return (
        SparkSession.builder
        .master("local[4]")                             
        .appName(app_name)

        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )