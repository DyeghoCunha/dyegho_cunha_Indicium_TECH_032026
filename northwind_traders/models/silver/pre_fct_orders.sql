{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'ord_order_id',
  tags = ['silver', 'fato']
) }}

SELECT
  COALESCE(CAST(order_id AS INT), -1) AS ord_order_id,
  COALESCE(CAST(customer_id AS STRING), {{ var('nao_inf') }}) AS ctm_customer_id,
  COALESCE(CAST(employee_id AS INT), -1) AS emp_employee_id,
  COALESCE(CAST(ship_via AS INT), -1) AS shp_shipper_id,
  COALESCE(CAST(order_date AS DATE), {{ var('date_default') }}) AS ord_order_date,
  COALESCE(CAST(required_date AS DATE), {{ var('date_default') }}) AS ord_required_date,
  CAST(
    shipped_date AS DATE
  ) AS ord_shipped_date,
  COALESCE(CAST(freight AS DECIMAL(10, 2)), 0.0) AS ord_freight,
  COALESCE(CAST(ship_name AS STRING), {{ var('nao_inf') }}) AS ord_ship_name,
  COALESCE(CAST(ship_address AS STRING), {{ var('nao_inf') }}) AS ord_ship_address,
  COALESCE(CAST(ship_city AS STRING), {{ var('nao_inf') }}) AS ord_ship_city,
  COALESCE(CAST(ship_region AS STRING), {{ var('nao_inf') }}) AS ord_ship_region,
  COALESCE(CAST(ship_postal_code AS STRING), {{ var('nao_inf') }}) AS ord_ship_postal_code,
  COALESCE(CAST(ship_country AS STRING), {{ var('nao_inf') }}) AS ord_ship_country,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS ord_insert_date
FROM
  {{ ref(
    'brz_erp_orders'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
