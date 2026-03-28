{{ config(
    materialized='view',
    schema='gold',
    tags=['gold', 'customer', 'cohort', 'view']
) }}

WITH base AS (
    SELECT 
        cohort_month,
        cohort_index,
        initial_customers,
        active_customers
    FROM {{ ref('mart_cohort_retention_base') }}
)

SELECT 
    cohort_month,
    cohort_index,
    initial_customers,
    active_customers,
    
    CAST(active_customers AS DECIMAL(10,4)) / NULLIF(initial_customers, 0) AS retention_rate,
    
    CURRENT_TIMESTAMP() AS view_calculated_at
FROM base