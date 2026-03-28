{{ config(
  materialized = 'view',
  schema = 'bronze',
  tags = ['bronze', 'ingestion']
) }}

SELECT
  supplier_id,
  company_name,
  contact_name,
  contact_title,
  address,
  city,
  region,
  postal_code,
  country,
  phone,
  fax,
  homepage,
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'suppliers'
  ) }}
