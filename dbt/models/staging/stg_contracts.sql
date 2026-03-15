WITH base AS (
    SELECT
        NULLIF(TRIM(CAST(contract AS VARCHAR)), 'NULL') AS contract,
        NULLIF(TRIM(CAST(user_id AS VARCHAR)), 'NULL') AS user_id,
        NULLIF(TRIM(CAST(mac AS VARCHAR)), 'NULL') as mac,
        ROW_NUMBER() OVER (PARTITION BY contract ORDER BY mac DESC) as row_num
    FROM {{ source('raw', 'contracts') }}
)

SELECT
    contract,
    user_id,
    mac
FROM base
WHERE row_num = 1
  AND contract IS NOT NULL
  AND user_id IS NOT NULL