"""
Explicit Spark schemas for the raw source files.
"""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


GOOGLE_SCHEMA = StructType([
    StructField("review_id", StringType(), nullable=False),
    StructField("user_name", StringType(), nullable=True),
    StructField("review_title", StringType(), nullable=True),            
    StructField("review_description", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=False),
    StructField("thumbs_up", IntegerType(), nullable=True),
    StructField("review_date", StringType(), nullable=False),            
    StructField("developer_response", StringType(), nullable=True),      
    StructField("developer_response_date", StringType(), nullable=True),
    StructField("appVersion", StringType(), nullable=True),              
    StructField("laguage_code", StringType(), nullable=True),            # the typo is in the source file
    StructField("country_code", StringType(), nullable=True),
])

# App Store JSON (one big array of objects). JSON fields are matched by NAME, so order does not matter.
IOS_SCHEMA = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("app_id", LongType(), nullable=True),                    # Long: app IDs can exceed the 32-bit range
    StructField("app_name", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("date", StringType(), nullable=False),                   
    StructField("userName", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("review", StringType(), nullable=True),
    StructField("rating", IntegerType(), nullable=False),
    StructField("isEdited", StringType(), nullable=True),                
    StructField("crawled_at", StringType(), nullable=True),              
    StructField("url", StringType(), nullable=True),
    # Present on only 51 records. The whole struct is null on the rest.
    StructField("developerResponse", StructType([
        StructField("id", LongType(), nullable=True),
        StructField("body", StringType(), nullable=True),
        StructField("modified", StringType(), nullable=True),            
    ]), nullable=True),
])