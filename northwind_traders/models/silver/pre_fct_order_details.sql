{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['ord_order_id', 'prd_product_id'],
  tags = ['silver', 'fato']
) }}

SELECT
  COALESCE(CAST(order_id AS INT), -1) AS ord_order_id,
  COALESCE(CAST(product_id AS INT), -1) AS prd_product_id,
  COALESCE(CAST(unit_price AS DECIMAL(10, 2)), 0.0) AS odt_unit_price,
  COALESCE(CAST(quantity AS INT), 0) AS odt_quantity,
  COALESCE(CAST(discount AS DECIMAL(10, 2)), 0.0) AS odt_discount,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS odt_insert_date
FROM
  {{ ref(
    'brz_erp_order_details'
  ) }}

{% if is_incremental() %}
WHERE
  CAST(
    _insert_date AS TIMESTAMP
  ) > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
