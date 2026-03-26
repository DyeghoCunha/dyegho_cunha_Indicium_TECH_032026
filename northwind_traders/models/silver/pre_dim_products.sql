{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'prd_product_id',
  tags = ['silver']
) }}

SELECT
  COALESCE(CAST(product_id AS INT), -1) AS prd_product_id,
  COALESCE(CAST(product_name AS STRING), {{ var('nao_inf') }}) AS prd_product_name,
  COALESCE(CAST(supplier_id AS INT), -1) AS sup_supplier_id,
  COALESCE(CAST(category_id AS INT), -1) AS cat_category_id,
  COALESCE(CAST(quantity_per_unit AS STRING), {{ var('nao_inf') }}) AS prd_quantity_per_unit,
  COALESCE(CAST(unit_price AS DECIMAL(10, 2)), 0.0) AS prd_unit_price,
  COALESCE(CAST(units_in_stock AS INT), 0) AS prd_units_in_stock,
  COALESCE(CAST(units_on_order AS INT), 0) AS prd_units_on_order,
  COALESCE(CAST(reorder_level AS INT), 0) AS prd_reorder_level,
  COALESCE(CAST(discontinued AS INT), 0) AS prd_is_discontinued,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS prd_insert_date
FROM
  {{ ref(
    'brz_erp_products'
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
