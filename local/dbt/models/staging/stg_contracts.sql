WITH base AS (
    SELECT
        contract,
        user_id,
        mac,
        ROW_NUMBER() OVER (PARTITION BY contract ORDER BY mac DESC) as row_num
    FROM {{ source('raw', 'contracts') }}
)

SELECT
    contract,
    mac,
    user_id
FROM base
WHERE row_num = 1
