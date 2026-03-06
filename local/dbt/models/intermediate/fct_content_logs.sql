{{ config(materialized='table') }}

SELECT 
    log_id,
    log_date,
    contract,
    mac_address,
    app_name,
    total_duration
FROM {{ ref('stg_log_content') }}
WHERE contract IS NOT NULL