{{ config(
  materialized = 'view',
  schema = 'bronze',
  tags = ['bronze', 'ingestion']
) }}

SELECT
  order_id,
  product_id,
  unit_price,
  quantity,
  discount,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'order_details'
  ) }}
