CREATE OR REPLACE TABLE `bigdata.customer_search_stats`
PARTITION BY DATE_TRUNC(month_date, MONTH) 
AS 

WITH raw_data AS (
  SELECT
    user_id,
    keyword,
    PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_FILE_NAME, r'(\d{8})')) AS month_date
  FROM `bigdata.log_search`
  WHERE user_id IS NOT NULL 
    AND keyword IS NOT NULL
),

keyword_counts AS (
  SELECT
    month_date,
    user_id,
    keyword,
    COUNT(*) AS search_count
  FROM raw_data
  GROUP BY month_date, user_id, keyword
)

SELECT
  month_date,
  user_id,
  keyword AS mostWatch
FROM keyword_counts
QUALIFY ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC(month_date, MONTH), user_id ORDER BY search_count DESC) = 1;