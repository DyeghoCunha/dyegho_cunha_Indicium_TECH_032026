{{ config(materialized='table', schema='gold', tags=['gold', 'customer', 'metrics']) }}

WITH customer_dim AS (
  SELECT * FROM {{ ref('dim_customers') }}
),

metrics AS (
  SELECT * FROM {{ ref('int_customer_metrics') }}
),


customer_demo_bridge AS (
  SELECT * FROM {{ ref('fct_customer_demo') }}
),

demographics_dim AS (
  SELECT * FROM {{ ref('dim_customer_demographics') }}
)

SELECT 
  d.sk_customer,
  d.nk_customer_id,
  d.ctm_company_name,
  d.ctm_city,
  d.ctm_region,
  d.ctm_country,
  
 
  m.first_order_date,
  m.last_order_date,
  m.recency_days,
  m.customer_tenure_days,
  

  m.total_orders,
  m.orders_last_30d,
  m.orders_last_90d,
  m.orders_last_365d,
  CASE 
    WHEN m.customer_tenure_days > 0 
    THEN CAST(m.total_orders AS DECIMAL(10,2)) / (CAST(m.customer_tenure_days AS DECIMAL(10,2)) / 30.0)
    ELSE 0 
  END AS avg_orders_per_month,
  
 
  m.total_revenue,
  m.revenue_last_30d,
  m.revenue_last_90d,
  m.avg_order_value,
  m.total_quantity,
  m.avg_discount_pct,
  m.avg_freight_per_order,
  m.inter_purchase_interval_days,
  m.late_deliveries,
  
  cdem.nk_customer_type_id AS rfm_code,        
  cdem.cdm_customer_desc AS customer_segment,  
  
  m.total_revenue AS customer_lifetime_value,
  
  CASE 
    WHEN m.inter_purchase_interval_days IS NOT NULL 
    THEN DATE_ADD(m.last_order_date, CAST(m.inter_purchase_interval_days AS INT))
    ELSE NULL
  END AS predicted_next_order_date,
  
  CASE 
    WHEN m.recency_days > 180 THEN 0.95
    WHEN m.recency_days > 120 THEN 0.80
    WHEN m.recency_days > 90 THEN 0.60
    WHEN m.recency_days > 30 THEN 0.15
    ELSE 0.05
  END AS churn_risk_score,
  
  CASE WHEN m.recency_days > 90 THEN TRUE ELSE FALSE END AS is_churned,
  
  CASE 
    WHEN cdem.cdm_customer_desc IN ('CHAMPIONS', 'LOYAL_CUSTOMERS') THEN 0.95
    WHEN cdem.cdm_customer_desc IN ('POTENTIAL_LOYALIST', 'PROMISING') THEN 0.75
    WHEN cdem.cdm_customer_desc IN ('NEW_CUSTOMERS', 'BIG_SPENDERS') THEN 0.60
    WHEN cdem.cdm_customer_desc IN ('NEED_ATTENTION', 'HIBERNATING') THEN 0.40
    WHEN cdem.cdm_customer_desc IN ('AT_RISK', 'CANNOT_LOSE_THEM') THEN 0.20
    WHEN cdem.cdm_customer_desc = 'LOST' THEN 0.05
    ELSE 0.50
  END AS customer_health_score,
  
  CURRENT_TIMESTAMP() AS calculated_at

FROM customer_dim d
LEFT JOIN metrics m 
  ON d.sk_customer = m.sk_customer
  
LEFT JOIN customer_demo_bridge fcd 
  ON d.sk_customer = fcd.sk_customer
LEFT JOIN demographics_dim cdem 
  ON fcd.sk_customer_demographic = cdem.sk_customer_demographic