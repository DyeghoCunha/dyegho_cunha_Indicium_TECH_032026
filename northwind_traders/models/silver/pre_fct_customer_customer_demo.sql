{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['ctm_customer_id', 'cdm_customer_type_id'],
  tags = ['silver', 'fato']
) }}

SELECT
  COALESCE(CAST(customer_id AS STRING), {{ var('nao_inf') }}) AS ctm_customer_id,
  COALESCE(CAST(customer_type_id AS STRING), {{ var('nao_inf') }}) AS cdm_customer_type_id,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS ccd_insert_date
FROM
  {{ ref(
    'brz_erp_customer_customer_demo'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
