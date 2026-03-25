{{ config(
  materialized = 'view',
  tags = ['bronze', 'erp_northwind']
) }}

SELECT
  employee_id,
  territory_id,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'employee_territories'
  ) }}
