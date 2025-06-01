WITH dim_country AS (
    SELECT DISTINCT
        country_id,
        country_name
    FROM {{ source('landing', 'country') }}
)
SELECT *
FROM dim_country