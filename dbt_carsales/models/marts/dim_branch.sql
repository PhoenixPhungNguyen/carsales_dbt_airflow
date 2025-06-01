WITH dim_branch AS (
    SELECT DISTINCT
        branch_id,
        branch_nm AS branch_name,
        country_name
    FROM {{ source('landing', 'branch') }}
)
SELECT *
FROM dim_branch