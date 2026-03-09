WITH raw_data AS (
    SELECT
        NULLIF(TRIM(CAST(log_user_id as VARCHAR)), 'NULL') as user_id,
        created_date
    FROM {{ source('raw', 'user_registrations') }}
    WHERE user_id IS NOT NULL
),

deduped_by_user AS (
    SELECT
        user_id,
        MIN(created_date) as created_date
    FROM raw_data
    GROUP BY user_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as id,
    user_id,
    created_date
FROM deduped_by_user
