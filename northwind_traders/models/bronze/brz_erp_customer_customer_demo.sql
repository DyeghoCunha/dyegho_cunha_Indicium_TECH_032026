{{ config(
  materialized = 'view',
  tags = ['bronze', 'erp_northwind']
) }}

SELECT
  customer_id,
  customer_type_id,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'customer_customer_demo'
  ) }}
