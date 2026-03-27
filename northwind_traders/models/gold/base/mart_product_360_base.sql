{{ config(
    materialized='incremental',
    unique_key='sk_product',
    incremental_strategy='merge',
    schema='gold',
    tags=['gold', 'product', 'base']
) }}

WITH changed_products AS (
  SELECT DISTINCT sk_product 
  FROM {{ ref('fct_order_details') }}
  {% if is_incremental() %}
  WHERE gold_insert_date >= CURRENT_DATE() - INTERVAL 3 DAY
  {% endif %}
),

dim_products AS (
  SELECT 
    sk_product,
    nk_product_id,
    prd_product_name,
    cat_category_name,
    sup_company_name,
    prd_unit_price,
    prd_units_in_stock,
    prd_is_discontinued
  FROM {{ ref('dim_products') }}
),

metrics AS (
  SELECT 
    sk_product,
    total_quantity_sold,
    total_revenue,
    unique_customers,
    first_sale_date,
    last_sale_date,
    quantity_sold_last_30d,
    revenue_last_30d,
    revenue_last_90d
  FROM {{ ref('int_product_metrics') }}
  {% if is_incremental() %}
  WHERE sk_product IN (SELECT sk_product FROM changed_products)
  {% endif %}
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
  
  m.total_quantity_sold,
  m.total_revenue,
  m.unique_customers,
  m.first_sale_date,
  m.last_sale_date,
  m.quantity_sold_last_30d,
  m.revenue_last_30d,
  m.revenue_last_90d,
  
  CURRENT_TIMESTAMP() AS base_calculated_at
  
FROM dim_products p
LEFT JOIN metrics m ON p.sk_product = m.sk_product

{% if is_incremental() %}
WHERE p.sk_product IN (SELECT sk_product FROM changed_products)
{% endif %}