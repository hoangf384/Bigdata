CREATE OR REPLACE TABLE `bigdata.customer_taste_final` AS

WITH month_6 AS (
  SELECT t.user_id, t.mostWatch AS mostWatch_m6, m.category_std AS category_m6
  FROM `bigdata.customer_search_stats` t
  LEFT JOIN `bigdata.mapping_std` m ON t.mostWatch = m.keyword
  WHERE t.month_date = '2022-06-01'
),

month_7 AS (
  SELECT t.user_id, t.mostWatch AS mostWatch_m7, m.category_std AS category_m7
  FROM `bigdata.customer_search_stats` t
  LEFT JOIN `bigdata.mapping_std` m ON t.mostWatch = m.keyword
  WHERE t.month_date = '2022-07-01'
)

SELECT
  COALESCE(m6.user_id, m7.user_id) AS user_id,
  m6.mostWatch_m6,
  m7.mostWatch_m7,
  m6.category_m6,
  m7.category_m7,
  IF(m6.category_m6 = m7.category_m7, 'Unchanged', 'Changed') AS taste
FROM month_6 m6
FULL OUTER JOIN month_7 m7 ON m6.user_id = m7.user_id;
