SELECT
    contract,
    mac,
    user_id
FROM {{ ref('stg_contracts') }}
