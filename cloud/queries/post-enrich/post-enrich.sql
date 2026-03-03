CREATE OR REPLACE TABLE `bigdata.customer_taste_final` AS

WITH month_6 AS (
  SELECT 
    t.user_id, 
    t.most_search AS most_search_m6, 
    COALESCE(m.category_std, 'Other') AS category_m6
  FROM `bigdata.customer_search_stats` t
  LEFT JOIN `bigdata.mapping_std` m 
    ON LOWER(TRIM(t.most_search)) = LOWER(TRIM(m.keyword))
  WHERE t.month_date = '2022-06-01'
),

month_7 AS (
  SELECT 
    t.user_id, 
    t.mostSearch AS mostSearch_m7, 
    COALESCE(m.category_std, 'Other') AS category_m7
  FROM `bigdata.customer_search_stats` t
  LEFT JOIN `bigdata.mapping_std` m 
    ON LOWER(TRIM(t.mostSearch)) = LOWER(TRIM(m.keyword))
  WHERE t.month_date = '2022-07-01'
)

SELECT
  COALESCE(m6.user_id, m7.user_id) AS user_id,
  m6.mostSearch_m6,
  m7.mostSearch_m7,
  m6.category_m6,
  m7.category_m7,
  CASE 
    WHEN m6.mostSearch_m6 IS NULL THEN 'New'
    WHEN m7.mostSearch_m7 IS NULL THEN 'Churned'
    WHEN m6.category_m6 = m7.category_m7 THEN 'Unchanged'
    ELSE 'Changed'
  END AS taste
FROM month_6 m6
FULL OUTER JOIN month_7 m7 ON m6.user_id = m7.user_id;
