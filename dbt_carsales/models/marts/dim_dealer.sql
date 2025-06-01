WITH dim_dealer AS (
    SELECT DISTINCT
        dealer_id,
        dealer_nm AS dealer_name,
        location_id,
        location_nm AS location_name,
        country_id
    FROM {{ source('landing', 'dealer') }}
)
SELECT *
FROM dim_dealer