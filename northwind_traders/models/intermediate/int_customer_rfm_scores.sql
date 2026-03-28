{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'customer'],
    cluster_by=['customer_id'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'true',
        'delta.autoOptimize.optimizeWrite': 'true'
    }
) }}

WITH customer_orders AS (
  SELECT 
    ctm_customer_id AS customer_id,
    ord_order_date AS order_date,
    ord_order_id AS order_id,
    ord_freight AS freight
  FROM {{ ref('pre_fct_orders') }}
),

order_details AS (
  SELECT 
    ord_order_id AS order_id,
    odt_unit_price * odt_quantity * (1 - odt_discount) AS line_total
  FROM {{ ref('pre_fct_order_details') }}
),

order_values AS (
  SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    SUM(od.line_total) AS order_value
  FROM customer_orders o
  LEFT JOIN order_details od ON o.order_id = od.order_id
  GROUP BY o.order_id, o.customer_id, o.order_date
),

customer_metrics AS (
  SELECT 
    customer_id,
    
    MAX(order_date) AS last_order_date,
    DATEDIFF(CAST('{{ var("current_business_date") }}' AS DATE), MAX(order_date)) AS recency_days,
    
    COUNT(DISTINCT order_id) AS frequency,
    
    SUM(order_value) AS monetary,
    
    MIN(order_date) AS first_order_date,
    AVG(order_value) AS avg_order_value,
    DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days
    
  FROM order_values
  GROUP BY customer_id
),

rfm_scores AS (
  SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    first_order_date,
    last_order_date,
    avg_order_value,
    customer_tenure_days,

    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    
  FROM customer_metrics
)

SELECT 
  customer_id,
  recency_days,
  frequency,
  monetary,
  first_order_date,
  last_order_date,
  avg_order_value,
  customer_tenure_days,
  r_score,
  f_score,
  m_score,
  CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_code,
  CURRENT_TIMESTAMP() AS calculated_at
FROM rfm_scores