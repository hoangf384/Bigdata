WITH mapping_data AS (
    SELECT
        keyword,
        category_std
    FROM {{ ref('stg_keyword_mapping') }}
)

-- Giữ lại QUALIFY để đảm bảo mỗi keyword (LOWER + TRIM) chỉ có duy nhất 1 category
SELECT
    keyword,
    category_std
FROM mapping_data
QUALIFY ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(keyword)) ORDER BY category_std DESC) = 1
