{{ config(
    materialized = 'table',
    schema = 'gold',
    liquid_clustered_by = ['sk_product_a', 'sk_product_b'],
    tags = ['gold', 'product', 'market_basket', 'base'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH order_items AS (

    SELECT
        sk_order,
        sk_product
    FROM
        {{ ref('fct_order_details') }}
),
product_pairs AS (
    SELECT
        A.sk_product AS sk_product_a,
        b.sk_product AS sk_product_b,
        COUNT(
            DISTINCT A.sk_order
        ) AS times_bought_together
    FROM
        order_items A
        JOIN order_items b
        ON A.sk_order = b.sk_order
        AND A.sk_product < b.sk_product
    GROUP BY
        A.sk_product,
        b.sk_product
)
SELECT
    sk_product_a,
    sk_product_b,
    times_bought_together,
    CURRENT_TIMESTAMP() AS base_calculated_at
FROM
    product_pairs
WHERE
    times_bought_together > {{ var(
        'min_basket_frequency',
        1
    ) }}
