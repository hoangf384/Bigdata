CREATE OR REPLACE TABLE `bigdata.customer_content_stats` AS

WITH raw_data AS (
  SELECT
    _source.Contract AS Contract,
    _source.AppName AS AppName,
    CAST(_source.TotalDuration AS INT64) AS TotalDuration,
    PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_FILE_NAME, r'(\d{8})\.json')) AS LogDate
  FROM `bigdata.log_content`
),

mapped_data AS (
  SELECT
    Contract,
    LogDate,
    TotalDuration,
    CASE
      WHEN AppName IN ('CHANNEL', 'DSHD', 'KPLUS', 'KPlus') THEN 'Truyen Hinh'
      WHEN AppName IN ('VOD', 'FIMS_RES', 'BHD_RES', 'VOD_RES', 'FIMS', 'BHD', 'DANET') THEN 'Phim Truyen'
      WHEN AppName = 'RELAX' THEN 'Giai Tri'
      WHEN AppName = 'CHILD' THEN 'Thieu Nhi'
      WHEN AppName = 'SPORT' THEN 'The Thao'
      ELSE 'Error'
    END AS Type
  FROM raw_data
  WHERE Contract IS NOT NULL AND Contract != '0'
),

filtered_data AS (
  SELECT * FROM mapped_data WHERE Type != 'Error'
),

aggregated_data AS (
  SELECT
    Contract,
    COUNT(DISTINCT LogDate) AS Active,
    SUM(IF(Type = 'Giai Tri', TotalDuration, 0)) AS Giai_Tri,
    SUM(IF(Type = 'Phim Truyen', TotalDuration, 0)) AS Phim_Truyen,
    SUM(IF(Type = 'The Thao', TotalDuration, 0)) AS The_Thao,
    SUM(IF(Type = 'Thieu Nhi', TotalDuration, 0)) AS Thieu_Nhi,
    SUM(IF(Type = 'Truyen Hinh', TotalDuration, 0)) AS Truyen_Hinh
  FROM filtered_data
  GROUP BY Contract
)

SELECT
  Contract,
  Active,
  Giai_Tri,
  Phim_Truyen,
  The_Thao,
  Thieu_Nhi,
  Truyen_Hinh,

  CASE
    WHEN Truyen_Hinh = GREATEST(Giai_Tri, Phim_Truyen, The_Thao, Thieu_Nhi, Truyen_Hinh) THEN 'Truyen Hinh'
    WHEN Phim_Truyen = GREATEST(Giai_Tri, Phim_Truyen, The_Thao, Thieu_Nhi, Truyen_Hinh) THEN 'Phim Truyen'
    WHEN The_Thao = GREATEST(Giai_Tri, Phim_Truyen, The_Thao, Thieu_Nhi, Truyen_Hinh) THEN 'The Thao'
    WHEN Thieu_Nhi = GREATEST(Giai_Tri, Phim_Truyen, The_Thao, Thieu_Nhi, Truyen_Hinh) THEN 'Thieu Nhi'
    WHEN Giai_Tri = GREATEST(Giai_Tri, Phim_Truyen, The_Thao, Thieu_Nhi, Truyen_Hinh) THEN 'Giai Tri'
  END AS MostWatch,

  ARRAY_TO_STRING(
    [
      IF(Giai_Tri > 0, 'Giai Tri', NULL),
      IF(Phim_Truyen > 0, 'Phim Truyen', NULL),
      IF(The_Thao > 0, 'The Thao', NULL),
      IF(Thieu_Nhi > 0, 'Thieu Nhi', NULL),
      IF(Truyen_Hinh > 0, 'Truyen Hinh', NULL)
    ],
    '-'
  ) AS Taste

FROM aggregated_data;
