SELECT *
FROM {{ ref('stg_user_views') }}
WHERE user_id IS NOT NULL
  AND user_id != 'NULL'
  AND content_name IS NOT NULL
