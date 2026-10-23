"""Explicit Spark schemas for the raw source files.

These describe the files exactly as delivered: same column names (typos
included), same column order for the CSV, and dates left as strings.
Renaming, date parsing, and type conversion happen in cleaning.py.

nullable=False marks the key fields as intent only. Spark relaxes nullability
for file sources (printSchema shows nullable = true), so real enforcement
happens in the data quality checks.
"""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Google Play CSV. A CSV schema is applied by POSITION, so the order must match the file's header.
GOOGLE_SCHEMA = StructType([
    StructField("review_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("review_title", StringType(), nullable=True),            # 100% null in this snapshot
    StructField("review_description", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=False),
    StructField("thumbs_up", IntegerType(), nullable=True),
    StructField("review_date", StringType(), nullable=False),            # e.g. 9/3/2024 0:23
    StructField("developer_response", StringType(), nullable=True),      # null when there is no response
    StructField("developer_response_date", StringType(), nullable=True),
    StructField("appVersion", StringType(), nullable=True),              # a version label, not a number
    StructField("laguage_code", StringType(), nullable=True),            # the typo is in the source file
    StructField("country_code", StringType(), nullable=True),
])

# App Store JSON (one big array of objects). JSON fields are matched by NAME, so order does not matter.
IOS_SCHEMA = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("app_id", LongType(), nullable=True),                    # Long: app IDs can exceed the 32-bit range
    StructField("app_name", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("date", StringType(), nullable=False),                   # e.g. 2017/9/8, 11:58:17
    StructField("userName", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("review", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=False),
    StructField("isEdited", StringType(), nullable=True),                # the strings 'true' / 'false'
    StructField("crawled_at", StringType(), nullable=True),              # e.g. 02/21/2021, 10:15:43
    StructField("url", StringType(), nullable=True),
    # Present on only 51 records. The whole struct is null on the rest.
    StructField("developerResponse", StructType([
        StructField("id", LongType(), nullable=True),
        StructField("body", StringType(), nullable=True),
        StructField("modified", StringType(), nullable=True),            # e.g. 2018-09-28T22:47:19Z
    ]), nullable=True),
])