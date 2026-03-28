{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_order',
    schema = 'gold',
    liquid_clustered_by = ['sk_order_date', 'sk_customer'],
    tags = ['gold', 'core', 'fact'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['o.ord_order_id']) }} AS sk_order,
        {{ dbt_utils.generate_surrogate_key(['o.ctm_customer_id']) }} AS sk_customer,
        {{ dbt_utils.generate_surrogate_key(['o.emp_employee_id']) }} AS sk_employee,
        {{ dbt_utils.generate_surrogate_key(['o.shp_shipper_id']) }} AS sk_shipper,
        CAST(date_format(o.ord_order_date, 'yyyyMMdd') AS INT) AS sk_order_date,
        CAST(date_format(o.ord_required_date, 'yyyyMMdd') AS INT) AS sk_required_date,
        CAST(date_format(o.ord_shipped_date, 'yyyyMMdd') AS INT) AS sk_shipped_date,
        o.ord_required_date,
        o.ord_shipped_date,
        o.ord_order_id AS nk_order_id,
        o.ord_order_date,
        o.ord_freight,
        o.ord_ship_name,
        o.ord_ship_city,
        o.ord_ship_region,
        o.ord_ship_country,
        o.bronze_insert_date,
        from_utc_timestamp(now(), 'GMT-3') AS gold_insert_date
    FROM
        {{ ref('pre_fct_orders') }}
        o

{% if is_incremental() %}
WHERE
    o.bronze_insert_date > (
        SELECT
            COALESCE(MAX(gold_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
        FROM
            {{ this }})
        {% endif %}
    )
SELECT
    sk_order,
    sk_customer,
    sk_employee,
    sk_shipper,
    sk_order_date,
    sk_required_date,
    sk_shipped_date,
    ord_required_date,
    ord_shipped_date,
    nk_order_id,
    ord_order_date,
    ord_freight,
    ord_ship_name,
    ord_ship_city,
    ord_ship_region,
    ord_ship_country,
    bronze_insert_date,
    gold_insert_date,
    CASE
        WHEN ord_shipped_date = CAST({{ var('date_default') }} AS TIMESTAMP) THEN -1
        ELSE DATEDIFF(
            ord_shipped_date,
            ord_order_date
        )
    END AS delivery_lead_time_days,
    CASE
        WHEN ord_shipped_date = CAST({{ var('date_default') }} AS TIMESTAMP) THEN -1
        ELSE DATEDIFF(
            ord_required_date,
            ord_shipped_date
        )
    END AS delivery_accuracy_days,
    CASE
        WHEN ord_shipped_date IS NULL
        OR ord_shipped_date = CAST({{ var('date_default') }} AS TIMESTAMP) THEN 'Pending'
        WHEN ord_shipped_date <= ord_required_date THEN 'On-Time'
        ELSE 'Late'END AS delivery_status,
        ord_freight AS freight_value
        FROM
            source
