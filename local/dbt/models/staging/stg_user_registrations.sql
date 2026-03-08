SELECT *
FROM {{ source('raw', 'user_registrations') }}
