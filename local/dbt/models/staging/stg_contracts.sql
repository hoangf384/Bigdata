WITH base AS (
    SELECT
        contract,
        Mac,
        user_id,
        ROW_NUMBER() OVER (PARTITION BY contract ORDER BY Mac DESC) as row_num
    FROM {{ source('raw', 'contracts') }}
)

SELECT
    contract,
    Mac,
    user_id
FROM base
WHERE row_num = 1
