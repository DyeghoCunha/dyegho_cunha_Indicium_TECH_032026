{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'ctm_customer_id',
  tags = ['silver']
) }}

SELECT
  COALESCE(CAST(customer_id AS STRING), {{ var('nao_inf') }}) AS ctm_customer_id,
  COALESCE(CAST(company_name AS STRING), {{ var('nao_inf') }}) AS ctm_company_name,
  COALESCE(CAST(contact_name AS STRING), {{ var('nao_inf') }}) AS ctm_contact_name,
  COALESCE(CAST(contact_title AS STRING), {{ var('nao_inf') }}) AS ctm_contact_title,
  COALESCE(CAST(address AS STRING), {{ var('nao_inf') }}) AS ctm_address,
  COALESCE(CAST(city AS STRING), {{ var('nao_inf') }}) AS ctm_city,
  COALESCE(CAST(region AS STRING), {{ var('nao_inf') }}) AS ctm_region,
  COALESCE(CAST(postal_code AS STRING), {{ var('nao_inf') }}) AS ctm_postal_code,
  COALESCE(CAST(country AS STRING), {{ var('nao_inf') }}) AS ctm_country,
  COALESCE(CAST(phone AS STRING), {{ var('nao_inf') }}) AS ctm_phone,
  COALESCE(CAST(fax AS STRING), {{ var('nao_inf') }}) AS ctm_fax,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS ctm_bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS ctm_insert_date
FROM
  {{ ref(
    'brz_erp_customers'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(ctm_bronze_insert_date), '1900-01-01')
    FROM
      {{ this }})
    {% endif %}
