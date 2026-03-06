{{ config(materialized='table') }}

WITH cleaned AS (

    SELECT
        eventID AS event_id,
        user_id,
        TRY_CAST(SUBSTR(datetime,1,23) AS TIMESTAMP) AS search_datetime,
        keyword,
        category,
        platform,
        networkType AS network_type
    FROM {{ ref('stg_log_search') }}

)

SELECT *
FROM cleaned
WHERE user_id IS NOT NULL
  AND user_id != 'NULL'
  AND search_datetime IS NOT NULL