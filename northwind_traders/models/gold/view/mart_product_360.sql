{{ config(
    materialized='view',
    schema='gold',
    tags=['gold', 'product', 'view']
) }}

WITH base AS (
  SELECT 
    sk_product,
    nk_product_id,
    prd_product_name,
    cat_category_name,
    sup_company_name,
    current_catalog_price,
    prd_units_in_stock,
    prd_is_discontinued,
    total_quantity_sold,
    total_revenue,
    unique_customers,
    first_sale_date,
    last_sale_date,
    quantity_sold_last_30d,
    revenue_last_30d,
    revenue_last_90d
  FROM {{ ref('mart_product_360_base') }}
)

SELECT 
  sk_product,
  nk_product_id,
  prd_product_name,
  cat_category_name,
  sup_company_name,
  CAST(COALESCE(current_catalog_price, 0) AS DECIMAL(10,2)) AS current_catalog_price,
  COALESCE(prd_units_in_stock, 0) AS prd_units_in_stock,
  COALESCE(prd_is_discontinued, 0) AS prd_is_discontinued,
  
  COALESCE(total_quantity_sold, 0) AS total_quantity_sold,
  COALESCE(unique_customers, 0) AS unique_customers,
  
  CAST(COALESCE(total_revenue, 0) AS DECIMAL(15,2)) AS total_revenue,
  
  first_sale_date,
  last_sale_date,
  
  COALESCE(DATEDIFF(CURRENT_DATE(), last_sale_date), 999) AS days_since_last_sale,
  
  CAST(
    CASE 
      WHEN COALESCE(quantity_sold_last_30d, 0) > 0 
      THEN prd_units_in_stock / (CAST(quantity_sold_last_30d AS DECIMAL(10,2)) / 30.0)
      ELSE 999.00
    END AS DECIMAL(10,2)
  ) AS days_of_inventory,
  
  CASE 
    WHEN COALESCE(quantity_sold_last_30d, 0) > 0 AND prd_units_in_stock < ((CAST(quantity_sold_last_30d AS DECIMAL(10,2)) / 30.0) * 7)
    THEN TRUE 
    ELSE FALSE 
  END AS is_stockout_risk,
  
  CASE 
    WHEN COALESCE(revenue_last_30d, 0) = 0 THEN 'NO_RECENT_SALES'
    WHEN COALESCE(revenue_last_90d, 0) = 0 THEN 'INTRODUCTION'
    WHEN (revenue_last_30d - (revenue_last_90d - revenue_last_30d) / 2.0) / NULLIF((revenue_last_90d - revenue_last_30d) / 2.0, 0) > 1.5 THEN 'GROWTH'
    WHEN (revenue_last_30d - (revenue_last_90d - revenue_last_30d) / 2.0) / NULLIF((revenue_last_90d - revenue_last_30d) / 2.0, 0) < 0.5 THEN 'DECLINE'
    ELSE 'MATURITY'
  END AS product_lifecycle_stage,
  
  CURRENT_TIMESTAMP() AS view_calculated_at

FROM base