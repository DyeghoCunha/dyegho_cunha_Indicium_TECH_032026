{{ config(
  materialized = 'view',
  tags = ['bronze', 'erp_northwind']
) }}

SELECT
  shipper_id,
  company_name,
  phone,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'shippers'
  ) }}
