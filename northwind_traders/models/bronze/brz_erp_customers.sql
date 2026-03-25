{{ config(
  materialized = 'view',
  tags = ['bronze', 'erp_northwind']
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
  {{ ref(
    'brz_erp_customers'
  ) }}
