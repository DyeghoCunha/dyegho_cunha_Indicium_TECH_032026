{{ config(materialized='table', schema='gold', tags=['gold', 'product', 'metrics']) }}

WITH dim_products AS (
  SELECT * FROM {{ ref('dim_products') }}
),

metrics AS (
  SELECT * FROM {{ ref('int_product_metrics') }}
)

SELECT 
  p.sk_product,
  p.nk_product_id,
  p.prd_product_name,
  p.cat_category_name,
  p.sup_company_name,
  p.prd_unit_price AS current_catalog_price,
  p.prd_units_in_stock,
  p.prd_is_discontinued,
  
  -- Vendas
  COALESCE(m.total_quantity_sold, 0) AS total_quantity_sold,
  COALESCE(m.total_revenue, 0) AS total_revenue,
  COALESCE(m.unique_customers, 0) AS unique_customers,
  
  -- Datas
  m.first_sale_date,
  m.last_sale_date,
  COALESCE(m.days_since_last_sale, 999) AS days_since_last_sale,
  
  -- Inventário
  CASE 
    WHEN m.quantity_sold_last_30d > 0 AND (CAST(m.quantity_sold_last_30d AS DECIMAL(10,2)) / 30.0) > 0
    THEN CAST(p.prd_units_in_stock AS DECIMAL(10,2)) / (CAST(m.quantity_sold_last_30d AS DECIMAL(10,2)) / 30.0)
    ELSE 999
  END AS days_of_inventory,
  
  -- Alertas
  CASE 
    WHEN m.quantity_sold_last_30d > 0 AND p.prd_units_in_stock < ((CAST(m.quantity_sold_last_30d AS DECIMAL(10,2)) / 30.0) * 7)
    THEN TRUE ELSE FALSE 
  END AS is_stockout_risk,
  
  CASE 
    WHEN m.revenue_last_30d IS NULL OR m.revenue_last_30d = 0 THEN 'NO_RECENT_SALES'
    WHEN m.revenue_last_90d IS NULL OR m.revenue_last_90d = 0 THEN 'INTRODUCTION'
    WHEN (m.revenue_last_30d - (m.revenue_last_90d - m.revenue_last_30d) / 2.0) / NULLIF((m.revenue_last_90d - m.revenue_last_30d) / 2.0, 0) > 1.5 THEN 'GROWTH'
    WHEN (m.revenue_last_30d - (m.revenue_last_90d - m.revenue_last_30d) / 2.0) / NULLIF((m.revenue_last_90d - m.revenue_last_30d) / 2.0, 0) < 0.5 THEN 'DECLINE'
    ELSE 'MATURITY'
  END AS product_lifecycle_stage,
  
  CURRENT_TIMESTAMP() AS calculated_at
  
FROM dim_products p
LEFT JOIN metrics m ON p.sk_product = m.sk_product