{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_order_detail',
    schema = 'gold',
    cluster_by = ['sk_order', 'sk_product'],
    tags = ['gold', 'core', 'fact'],
    tblproperties ={ 
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'auto',
        'delta.autoOptimize.optimizeWrite': 'true' 
    }
) }}

WITH source AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['od.ord_order_id', 'od.prd_product_id']) }} AS sk_order_detail,
        {{ dbt_utils.generate_surrogate_key(['od.ord_order_id']) }} AS sk_order,
        {{ dbt_utils.generate_surrogate_key(['od.prd_product_id']) }} AS sk_product,
        od.ord_order_id AS nk_order_id,
        od.prd_product_id AS nk_product_id,
        od.odt_unit_price,
        od.odt_quantity,
        od.odt_discount,
        (od.odt_unit_price * od.odt_quantity) AS gross_amount,
        (od.odt_unit_price * od.odt_quantity * (1 - od.odt_discount)) AS net_amount,
        (od.odt_unit_price * od.odt_quantity * od.odt_discount) AS discount_amount,
        od.bronze_insert_date,
        from_utc_timestamp(now(), 'GMT-3') AS gold_insert_date
    FROM
        {{ ref('pre_fct_order_details') }} od

    {% if is_incremental() %}
    WHERE od.bronze_insert_date > (
        SELECT 
            COALESCE(MAX(gold_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
        FROM 
            {{ this }}
    )
    {% endif %}
)

SELECT
    sk_order_detail,
    sk_order,
    sk_product,
    nk_order_id,
    nk_product_id,
    odt_unit_price,
    odt_quantity,
    odt_discount,
    gross_amount,
    net_amount,
    discount_amount,
    bronze_insert_date, 
    gold_insert_date
FROM
    source