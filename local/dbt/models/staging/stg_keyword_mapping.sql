SELECT
    keyword,
    category_std
FROM {{ source('raw', 'keyword_mapping') }}
