{{ config(materialized='view') }}

SELECT *
FROM {{ source('raw', 'log_search') }}
