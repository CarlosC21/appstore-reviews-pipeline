"""Create the SparkSession used by every Spark script in this project."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Return a local SparkSession configured for this project."""
    # Spark's Python workers must use the same interpreter as this script. On Windows the
    # default executable name, "python3", does not exist. Set before the session starts.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # On Windows, Hadoop's local file writes need winutils.exe and hadoop.dll. Java finds
    # hadoop.dll through PATH, so put HADOOP_HOME\bin first instead of relying on the user's PATH.
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
        .master("local[4]")                              # run inside this process, one worker per CPU core
        .appName(app_name)
        # The sources carry no timezone, so we ASSUME UTC. This is the one place that assumption lives.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )