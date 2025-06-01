WITH fact_revenue AS (
    SELECT
        dealer_id,
        model_id,
        branch_id,
        d2.date_id,
        units_sold::NUMERIC AS units_sold,
        revenue::NUMERIC AS revenue
    FROM {{ source('landing', 'revenue') }} r
    JOIN {{ source('landing', 'date') }} d1
    ON r.date_id = d1.date_id
    JOIN {{ ref('dim_date') }} d2
    ON TO_DATE(d1.date, 'DD/MM/YYYY') = d2.date
)
SELECT *
FROM fact_revenue
