{{ config(materialized='view') }}

SELECT *
FROM {{ source('raw', 'user_registrations') }}
