{{ config(
    materialized='table',
    schema='gold',
    liquid_clustered_by=['cohort_month', 'cohort_index'],
    tags=['gold', 'customer', 'cohort', 'base'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH customer_cohort AS (
    SELECT 
        sk_customer,
        DATE_TRUNC('month', CAST(first_order_date AS DATE)) AS cohort_month
    FROM {{ ref('mart_customer_360_base') }}
    WHERE first_order_date IS NOT NULL
),

customer_activity AS (
    SELECT DISTINCT 
        sk_customer,
        DATE_TRUNC('month', CAST(ord_order_date AS DATE)) AS activity_month
    FROM {{ ref('fct_orders') }}
    WHERE ord_order_date IS NOT NULL
),

cohort_calc AS (
    SELECT 
        c.sk_customer,
        c.cohort_month,
        a.activity_month,
        CAST(
            (YEAR(a.activity_month) - YEAR(c.cohort_month)) * 12 + 
            (MONTH(a.activity_month) - MONTH(c.cohort_month)) 
        AS INT) AS cohort_index
    FROM customer_cohort c
    JOIN customer_activity a ON c.sk_customer = a.sk_customer
),

cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT sk_customer) AS initial_customers
    FROM cohort_calc
    WHERE cohort_index = 0
    GROUP BY cohort_month
),

retention_base AS (
    SELECT 
        cohort_month,
        cohort_index,
        COUNT(DISTINCT sk_customer) AS active_customers
    FROM cohort_calc
    GROUP BY 
        cohort_month, 
        cohort_index
)

SELECT 
    r.cohort_month,
    r.cohort_index,
    s.initial_customers,
    r.active_customers,
    CURRENT_TIMESTAMP() AS base_calculated_at
FROM retention_base r
JOIN cohort_size s ON r.cohort_month = s.cohort_month
WHERE r.cohort_index >= 0