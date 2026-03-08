WITH cleaned AS (

    SELECT
        TRY_CAST(log_user_id AS INT) AS user_id,
        created_date
    FROM {{ ref('stg_user_registrations') }}

)

SELECT
    user_id,
    MIN(created_date) AS created_date
FROM cleaned
WHERE user_id IS NOT NULL
GROUP BY user_id
