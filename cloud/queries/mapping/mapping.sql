CREATE OR REPLACE TABLE `bigdata.mapping_std` AS
SELECT
  keyword,
  CASE
    WHEN category_std IN ('Action') THEN 'Action'
    WHEN category_std IN ('Romance') THEN 'Romance'
    WHEN category_std IN ('Comedy') THEN 'Comedy'
    WHEN category_std IN ('Horror') THEN 'Horror'
    WHEN category_std IN ('Animation', '.Animation', 'Animation/Drama') THEN 'Animation'
    WHEN category_std IN ('Drama', 'Drama/Other') THEN 'Drama'
    WHEN category_std IN ('C Drama') THEN 'C Drama'
    WHEN category_std IN ('K Drama', 'KDrama') THEN 'K Drama'
    WHEN category_std IN ('Sports', 'Sport', 'Live Sports', 'Sports/Drama') THEN 'Sports'
    WHEN category_std IN ('Music', 'Music/Drama') THEN 'Music'
    WHEN category_std IN ('Reality Show', 'Reality', 'Dance/Reality', 'Cooking/Reality') THEN 'Reality Show'
    WHEN category_std IN ('TV Channel', 'Live') THEN 'TV Channel'
    WHEN category_std IN ('News') THEN 'News'
    ELSE 'Other'
  END AS category_std
FROM `bigdata.raw_mapping`;
