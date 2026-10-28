CREATE DATABASE IF NOT EXISTS reviews_db;

-- columns, in the order as the Parquet files
CREATE TABLE IF NOT EXISTS reviews_db.reviews (

    review_id                STRING     COMMENT 'Unique within a platform. The key is (platform, review_id)',
    app_name                 STRING,
    app_id                   BIGINT     COMMENT 'App Store',
    user_name                STRING,
    review_title             STRING,
    review_text              STRING,
    rating                   INT        COMMENT '1 to 5',
    review_ts                TIMESTAMP  COMMENT 'Assumed UTC for sources without a timezone',
    review_date              DATE,
    review_length            INT        COMMENT 'Characters in review_text',
    thumbs_up                INT        COMMENT 'Google Play',
    is_edited                BOOLEAN    COMMENT 'App Store',
    app_version              STRING     COMMENT 'Google Play',
    language_code            STRING     COMMENT 'Google Play',
    country_code             STRING,
    developer_response_text  STRING,
    developer_response_ts    TIMESTAMP,
    has_developer_response   BOOLEAN,
    source_snapshot          STRING     COMMENT 'Raw snapshot this row came from',
    processed_at             TIMESTAMP  COMMENT 'When the Spark job ran'
)
USING PARQUET
PARTITIONED BY (platform STRING, review_year INT)
LOCATION '{location}'
COMMENT 'Cleaned app reviews from Google Play and the App Store, one row per review';

MSCK REPAIR TABLE reviews_db.reviews;

CREATE OR REPLACE VIEW reviews_db.v_rating_by_app_year AS
SELECT platform,
       app_name,
       review_year,
       COUNT(*)              AS review_count,
       ROUND(AVG(rating), 2) AS avg_rating
FROM reviews_db.reviews
GROUP BY platform, app_name, review_year;

CREATE OR REPLACE VIEW reviews_db.v_developer_response_rate
AS
SELECT platform,
       app_name,
       COUNT(*)                                                                  AS review_count,
       SUM(CASE WHEN has_developer_response THEN 1 ELSE 0 END)                   AS responded,
       ROUND(100.0 * SUM(CASE WHEN has_developer_response THEN 1 ELSE 0 END) / COUNT(*), 1) AS response_rate_pct
FROM reviews_db.reviews
GROUP BY platform, app_name;

CREATE OR REPLACE VIEW reviews_db.v_review_length_by_rating AS
SELECT platform,
       rating,
       COUNT(*)                    AS review_count,
       ROUND(AVG(review_length), 0) AS avg_review_length
FROM reviews_db.reviews
GROUP BY platform, rating;