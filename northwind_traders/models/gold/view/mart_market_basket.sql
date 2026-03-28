{{ config(
    materialized='view',
    schema='gold',
    tags=['gold', 'product', 'market_basket', 'view']
) }}

WITH basket_base AS (
    SELECT 
        sk_product_a,
        sk_product_b,
        times_bought_together
    FROM {{ ref('mart_market_basket_base') }}
),

products_dim AS (
    SELECT 
        sk_product,
        prd_product_name,
        cat_category_name
    FROM {{ ref('dim_products') }}
)

SELECT 
    b.sk_product_a,
    d1.prd_product_name AS product_a_name,
    d1.cat_category_name AS product_a_category,
    
    b.sk_product_b,
    d2.prd_product_name AS product_b_name,
    d2.cat_category_name AS product_b_category,
    
    b.times_bought_together,
    
    ROW_NUMBER() OVER (PARTITION BY b.sk_product_a ORDER BY b.times_bought_together DESC) AS recommendation_rank,
    
    CURRENT_TIMESTAMP() AS view_calculated_at
FROM basket_base b
JOIN products_dim d1 ON b.sk_product_a = d1.sk_product
JOIN products_dim d2 ON b.sk_product_b = d2.sk_product