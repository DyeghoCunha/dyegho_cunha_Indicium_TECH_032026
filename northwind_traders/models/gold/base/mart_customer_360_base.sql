{{ config(
  materialized = 'incremental',
  unique_key = 'sk_customer',
  incremental_strategy = 'merge',
  schema = 'gold',
  cluster_by = ['sk_customer'],
  tags = ['gold', 'customer', 'base'],
  tblproperties ={ 
    'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' 
  }
) }}

WITH changed_customers AS (
    SELECT DISTINCT 
        sk_customer
    FROM {{ ref('fct_orders') }}
    {% if is_incremental() %}
    WHERE gold_insert_date >= CURRENT_DATE() - INTERVAL 3 DAY
    {% endif %}
),

customer_dim AS (
    SELECT
        sk_customer,
        nk_customer_id,
        ctm_company_name,
        ctm_city,
        ctm_region,
        ctm_country
    FROM {{ ref('dim_customers') }}
    {% if is_incremental() %}
    WHERE sk_customer IN (SELECT sk_customer FROM changed_customers)
    {% endif %}
),

metrics AS (
    SELECT
        sk_customer,
        first_order_date,
        last_order_date,
        total_orders,
        total_revenue,
        avg_order_value,
        total_quantity,
        avg_discount_pct,
        avg_freight_per_order,
        inter_purchase_interval_days,
        late_deliveries
    FROM {{ ref('int_customer_metrics') }}
    {% if is_incremental() %}
    WHERE sk_customer IN (SELECT sk_customer FROM changed_customers)
    {% endif %}
),

customer_demo_bridge AS (
    SELECT
        sk_customer,
        sk_customer_demographic
    FROM {{ ref('fct_customer_demo') }}
    {% if is_incremental() %}
    WHERE sk_customer IN (SELECT sk_customer FROM changed_customers)
    {% endif %}
),

demographics_dim AS (
    SELECT
        sk_customer_demographic,
        nk_customer_type_id,
        cdm_customer_desc
    FROM {{ ref('dim_customer_demographics') }}
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
    m.total_orders,
    m.total_revenue,
    m.avg_order_value,
    m.total_quantity,
    m.avg_discount_pct,
    m.avg_freight_per_order,
    m.inter_purchase_interval_days,
    m.late_deliveries,
    cdem.nk_customer_type_id AS rfm_code,
    COALESCE(cdem.cdm_customer_desc, {{ var('nao_inf') }}) AS customer_segment,
    CURRENT_TIMESTAMP() AS base_calculated_at
FROM customer_dim d
LEFT JOIN metrics m 
    ON d.sk_customer = m.sk_customer
LEFT JOIN customer_demo_bridge fcd 
    ON d.sk_customer = fcd.sk_customer
LEFT JOIN demographics_dim cdem 
    ON fcd.sk_customer_demographic = cdem.sk_customer_demographic

{% if is_incremental() %}
WHERE d.sk_customer IN (SELECT sk_customer FROM changed_customers)
{% endif %}