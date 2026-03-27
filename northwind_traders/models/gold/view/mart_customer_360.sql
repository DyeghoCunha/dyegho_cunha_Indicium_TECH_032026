{{ config(
    materialized='view',
    schema='gold',
    tags=['gold', 'customer', 'view']
) }}

WITH base AS (
  SELECT 
    sk_customer,
    nk_customer_id,
    ctm_company_name,
    ctm_city,
    ctm_region,
    ctm_country,
    CAST(COALESCE(first_order_date, {{ var("date_default") }}) AS DATE) AS first_order_date,
    CAST(COALESCE(last_order_date, {{ var("date_default") }}) AS DATE) AS last_order_date,
    total_orders,
    total_revenue,
    avg_order_value,
    total_quantity,
    avg_discount_pct,
    rfm_code,
    customer_segment
  FROM {{ ref('mart_customer_360_base') }}
)

SELECT 
  sk_customer,
  nk_customer_id,
  ctm_company_name,
  ctm_city,
  ctm_region,
  ctm_country,
  
  first_order_date, 
  last_order_date, 

  CASE 
    WHEN last_order_date = CAST({{ var("date_default") }} AS DATE) THEN 9999
  ELSE DATEDIFF(CAST('{{ var("current_business_date") }}' AS DATE), last_order_date)
  END AS recency_days,
  
  CASE 
    WHEN first_order_date = CAST({{ var("date_default") }} AS DATE) THEN 0
    ELSE DATEDIFF(last_order_date, first_order_date) 
  END AS customer_tenure_days,
  
  COALESCE(total_orders, 0) AS total_orders,
  
  CAST(COALESCE(total_revenue, 0) AS DECIMAL(15,2)) AS total_revenue,
  CAST(COALESCE(avg_order_value, 0) AS DECIMAL(15,2)) AS avg_order_value,
  COALESCE(total_quantity, 0) AS total_quantity,
  CAST(COALESCE(avg_discount_pct, 0) AS DECIMAL(5,4)) AS avg_discount_pct,
  
  COALESCE(rfm_code, 'NEW') AS rfm_code,
  COALESCE(customer_segment, {{ var('nao_inf') }}) AS customer_segment,
  
  CAST(COALESCE(total_revenue, 0) AS DECIMAL(15,2)) AS customer_lifetime_value,
  
  CASE 
    WHEN DATEDIFF('{{ var("current_business_date") }}', last_order_date) > 90 THEN TRUE 
    ELSE FALSE 
  END AS is_churned,
  
  CAST(
    CASE 
      WHEN rfm_code IN ('CHAMPIONS', 'LOYAL_CUSTOMERS') THEN 0.95
      WHEN rfm_code IN ('POTENTIAL_LOYALIST', 'PROMISING') THEN 0.75
      WHEN rfm_code IN ('NEW_CUSTOMERS', 'BIG_SPENDERS') THEN 0.60
      WHEN rfm_code IN ('NEED_ATTENTION', 'HIBERNATING') THEN 0.40
      WHEN rfm_code IN ('AT_RISK', 'CANNOT_LOSE_THEM') THEN 0.20
      WHEN rfm_code = 'LOST' THEN 0.05
      ELSE 0.50 
    END AS DECIMAL(3,2)
  ) AS customer_health_score,
  
  CURRENT_TIMESTAMP() AS view_calculated_at

FROM base