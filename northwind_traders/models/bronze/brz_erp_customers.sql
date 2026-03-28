{{ config(
  materialized = 'view',
  schema = 'bronze',
  tags = ['bronze', 'ingestion']
) }}

SELECT
  customer_id,
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
  _source_file,
  _insert_date,
  _system_source
FROM
  {{ source(
    'erp_northwind',
    'customers'
  ) }}
