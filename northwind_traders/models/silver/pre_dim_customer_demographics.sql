{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'cdm_customer_type_id',
  tags = ['silver', 'dimensao']
) }}

SELECT
  COALESCE(CAST(customer_type_id AS STRING), {{ var('nao_inf') }}) AS cdm_customer_type_id,
  COALESCE(CAST(customer_desc AS STRING), {{ var('nao_inf') }}) AS cdm_customer_desc,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS cdm_insert_date
FROM
  {{ ref(
    'brz_erp_customer_demographics'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
