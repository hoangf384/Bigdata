SELECT
    contract,
    Mac,
    user_id
FROM {{ ref('stg_contracts') }}