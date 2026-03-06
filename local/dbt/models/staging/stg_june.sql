{{ config(materialized='view') }}

SELECT *
FROM {{ source('raw', 'thang6') }}
