{{ config(
    materialized = 'incremental',
    unique_key = 'sk_product',
    incremental_strategy = 'merge',
    schema = 'gold',
    liquid_clustered_by = ['sk_product'],
    tags = ['gold', 'product', 'base'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH changed_products AS (

    SELECT
        DISTINCT sk_product
    FROM
        {{ ref('fct_order_details') }}

{% if is_incremental() %}
WHERE
    gold_insert_date >= CURRENT_DATE() - INTERVAL 3 DAY
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
    FROM
        {{ ref('dim_products') }}

{% if is_incremental() %}
WHERE
    sk_product IN (
        SELECT
            sk_product
        FROM
            changed_products
    )
{% endif %}
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
    FROM
        {{ ref('int_product_metrics') }}

{% if is_incremental() %}
WHERE
    sk_product IN (
        SELECT
            sk_product
        FROM
            changed_products
    )
{% endif %}
),
calculated_features AS (
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
        CAST(COALESCE(m.quantity_sold_last_30d, 0) / 30.0 AS DECIMAL(10, 2)) AS avg_daily_sales_30d,
        CAST(
            CASE
                WHEN COALESCE(
                    m.quantity_sold_last_30d,
                    0
                ) > 0 THEN p.prd_units_in_stock / (
                    m.quantity_sold_last_30d / 30.0
                )
                ELSE 999
            END AS DECIMAL(
                10,
                1
            )
        ) AS estimated_days_of_stock
    FROM
        dim_products p
        LEFT JOIN metrics m
        ON p.sk_product = m.sk_product
)
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
    revenue_last_90d,
    avg_daily_sales_30d,
    estimated_days_of_stock,
    CASE
        WHEN prd_is_discontinued = 0
        AND estimated_days_of_stock <= 7 THEN 1
        ELSE 0
    END AS is_out_of_stock_risk,
    CURRENT_TIMESTAMP() AS base_calculated_at
FROM
    calculated_features

{% if is_incremental() %}
WHERE
    sk_product IN (
        SELECT
            sk_product
        FROM
            changed_products
    )
{% endif %}
