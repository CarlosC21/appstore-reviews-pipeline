"""Cleaning and conforming: turn the two raw sources into one review table.

clean_*    rename to snake_case, parse timestamps, cast types, flatten, deduplicate (per source)
conform_*  add platform, lineage, and derived columns (per source)
combine    union both sources into one table with a fixed column order
"""

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Spark datetime patterns: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# A single letter (M, d, H, m, s) accepts 1 or 2 digits. A doubled letter (MM, dd, HH) expects
# exactly 2, so a strict pattern doubles as a format check: if the source changes, parsing fails.
# The first three sources carry no timezone, so values are read in the session timezone (UTC).
GOOGLE_TS_FORMAT = "M/d/yyyy H:mm"                    # 9/3/2024 0:23 and 5/21/2024 23:50
IOS_REVIEW_TS_FORMAT = "yyyy/M/d, H:m:s"              # 2017/9/8, 11:58:17 (minutes and seconds are not padded)
IOS_CRAWLED_TS_FORMAT = "MM/dd/yyyy, HH:mm:ss"        # 02/21/2021, 10:15:43 (always padded, so strict on purpose)
IOS_RESPONSE_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"   # 2018-09-28T22:47:19Z (XXX reads the zone from the data)

# Final column order of the processed review table.
# The natural key is (platform, review_id): review IDs are only unique within a platform.
TARGET_COLUMNS = [
    "platform",
    "review_id",
    "app_name",
    "app_id",                     # App Store only
    "user_name",
    "review_title",
    "review_text",
    "rating",
    "review_ts",
    "review_date",                # derived, for daily rollups
    "review_year",                # derived, used as a partition column
    "review_length",              # derived: characters in the review text
    "thumbs_up",                  # Google only
    "is_edited",                  # App Store only
    "app_version",                # Google only
    "language_code",              # Google only
    "country_code",
    "developer_response_text",
    "developer_response_ts",
    "has_developer_response",     # derived
    "source_snapshot",            # lineage: which raw snapshot the row came from
    "processed_at",               # lineage: when the job ran
]


# --- Timestamp parsing -------------------------------------------------------------------------

def parse_ts(column: str, fmt: str) -> Column:
    """Parse a string column to a timestamp. A value that does not match becomes null.

    try_to_timestamp is used because Spark 4 runs in ANSI mode by default, where
    to_timestamp raises an error on a bad value. We want to count failures instead
    (see count_parse_failures), so the job decides what to do about them.
    """
    # try_to_timestamp takes its format as a column expression, so the pattern is wrapped in lit()
    return F.try_to_timestamp(F.col(column), F.lit(fmt))


def count_parse_failures(df: DataFrame, column: str, fmt: str) -> int:
    """Rows where the source value exists but did not parse to a timestamp."""
    return df.where(F.col(column).isNotNull() & parse_ts(column, fmt).isNull()).count()


def total_parse_failures(raw_google: DataFrame, raw_ios: DataFrame) -> int:
    """Total timestamp parse failures across both raw sources. Should be 0."""
    return (
        count_parse_failures(raw_google, "review_date", GOOGLE_TS_FORMAT)
        + count_parse_failures(raw_google, "developer_response_date", GOOGLE_TS_FORMAT)
        + count_parse_failures(raw_ios, "date", IOS_REVIEW_TS_FORMAT)
        + count_parse_failures(raw_ios, "crawled_at", IOS_CRAWLED_TS_FORMAT)
        + count_parse_failures(raw_ios, "developerResponse.modified", IOS_RESPONSE_TS_FORMAT)
    )


# --- Per-source cleaning -----------------------------------------------------------------------

def clean_google(df: DataFrame) -> DataFrame:
    """Clean the Google Play reviews."""
    return df.select(
        F.col("review_id"),
        F.col("user_name"),
        F.col("review_title"),                                    # 100% null today, kept for schema parity
        F.col("review_description").alias("review_text"),
        F.col("rating"),
        F.col("thumbs_up"),
        parse_ts("review_date", GOOGLE_TS_FORMAT).alias("review_ts"),
        F.col("developer_response").alias("developer_response_text"),
        parse_ts("developer_response_date", GOOGLE_TS_FORMAT).alias("developer_response_ts"),
        F.col("appVersion").alias("app_version"),
        F.col("laguage_code").alias("language_code"),             # fixes the typo in the source
        F.col("country_code"),
    )


def clean_ios(df: DataFrame) -> DataFrame:
    """Clean the App Store reviews: rename, cast, flatten the nested response, deduplicate."""
    cleaned = df.select(
        F.col("_id").alias("review_id"),
        F.col("app_id"),
        F.col("app_name"),
        F.col("country").alias("country_code"),
        F.col("userName").alias("user_name"),
        F.col("title").alias("review_title"),
        F.col("review").alias("review_text"),
        F.col("rating"),
        # A cast (not "== 'true'") so an unexpected value fails loudly instead of becoming false
        F.col("isEdited").cast("boolean").alias("is_edited"),
        parse_ts("date", IOS_REVIEW_TS_FORMAT).alias("review_ts"),
        parse_ts("crawled_at", IOS_CRAWLED_TS_FORMAT).alias("crawled_ts"),
        F.col("url"),
        # Flatten the nested struct with dot notation
        F.col("developerResponse.id").alias("developer_response_id"),
        F.col("developerResponse.body").alias("developer_response_text"),
        parse_ts("developerResponse.modified", IOS_RESPONSE_TS_FORMAT).alias("developer_response_ts"),
    )
    # The 3 duplicates are exact copies, so which one is kept does not matter.
    # dropDuplicates is a wide transformation: it shuffles data across partitions.
    return cleaned.dropDuplicates(["review_id"])


# --- Conforming and combining ------------------------------------------------------------------

def _add_derived_columns(df: DataFrame) -> DataFrame:
    """Columns derived from existing ones, shared by both sources."""
    return (
        df.withColumn("review_date", F.to_date("review_ts"))
        .withColumn("review_year", F.year("review_ts"))
        .withColumn("review_length", F.length("review_text"))
        .withColumn("has_developer_response", F.col("developer_response_text").isNotNull())
    )


def conform_google(df: DataFrame, app_name: str, source_snapshot: str) -> DataFrame:
    """Add platform, app name, lineage, and derived columns to the cleaned Google reviews.

    The Google file does not say which app it covers, so the name comes from config.yaml.
    """
    return _add_derived_columns(
        df.withColumn("platform", F.lit("google_play"))
        .withColumn("app_name", F.lit(app_name))
        .withColumn("source_snapshot", F.lit(source_snapshot))
    )


def conform_ios(df: DataFrame, source_snapshot: str) -> DataFrame:
    """Add platform, lineage, and derived columns to the cleaned App Store reviews."""
    return _add_derived_columns(
        df.withColumn("platform", F.lit("app_store"))
        .withColumn("source_snapshot", F.lit(source_snapshot))
        # Dropped on purpose: kept in the raw layer, not needed in the modeled table
        .drop("crawled_ts", "url", "developer_response_id")
    )


def combine(google: DataFrame, ios: DataFrame, processed_at: datetime) -> DataFrame:
    """Union both sources into one table with a fixed column order.

    unionByName matches columns by name, and allowMissingColumns=True fills columns that
    exist on only one side with nulls (app_id and is_edited for Google; thumbs_up,
    app_version and language_code for the App Store).
    """
    combined = google.unionByName(ios, allowMissingColumns=True)
    # One timestamp for the whole run, set in Python so every row carries the same value
    return combined.withColumn("processed_at", F.lit(processed_at)).select(*TARGET_COLUMNS)