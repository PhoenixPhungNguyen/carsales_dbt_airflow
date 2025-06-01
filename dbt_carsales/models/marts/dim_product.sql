WITH dim_product AS (
    SELECT DISTINCT
        product_id,
        product_name,
        model_id,
        model_name
    FROM {{ source('landing', 'product') }}
)
SELECT *
FROM dim_product