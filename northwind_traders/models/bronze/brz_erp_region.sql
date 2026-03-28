{{ config(
  materialized = 'view',
  schema = 'bronze',
  tags = ['bronze', 'ingestion']
) }}

SELECT
  region_id,
  region_description,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'region'
  ) }}
