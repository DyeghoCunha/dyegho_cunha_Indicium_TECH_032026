{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'sup_supplier_id',
  tags = ['silver', 'dimensao']
) }}

SELECT
  COALESCE(CAST(supplier_id AS INT), -1) AS sup_supplier_id,
  COALESCE(CAST(company_name AS STRING), {{ var('nao_inf') }}) AS sup_company_name,
  COALESCE(CAST(contact_name AS STRING), {{ var('nao_inf') }}) AS sup_contact_name,
  COALESCE(CAST(contact_title AS STRING), {{ var('nao_inf') }}) AS sup_contact_title,
  COALESCE(CAST(address AS STRING), {{ var('nao_inf') }}) AS sup_address,
  COALESCE(CAST(city AS STRING), {{ var('nao_inf') }}) AS sup_city,
  COALESCE(CAST(region AS STRING), {{ var('nao_inf') }}) AS sup_region,
  COALESCE(CAST(postal_code AS STRING), {{ var('nao_inf') }}) AS sup_postal_code,
  COALESCE(CAST(country AS STRING), {{ var('nao_inf') }}) AS sup_country,
  COALESCE(CAST(phone AS STRING), {{ var('nao_inf') }}) AS sup_phone,
  COALESCE(CAST(fax AS STRING), {{ var('nao_inf') }}) AS sup_fax,
  COALESCE(CAST(homepage AS STRING), {{ var('nao_inf') }}) AS sup_homepage,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS sup_insert_date
FROM
  {{ ref(
    'brz_erp_suppliers'
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
