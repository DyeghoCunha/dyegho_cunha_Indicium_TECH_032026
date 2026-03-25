{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'ust_state_id',
  tags = ['silver', 'dimensao']
) }}

SELECT
  COALESCE(CAST(state_id AS INT), -1) AS ust_state_id,
  COALESCE(CAST(state_name AS STRING), {{ var('nao_inf') }}) AS ust_state_name,
  COALESCE(CAST(state_abbr AS STRING), {{ var('nao_inf') }}) AS ust_state_abbr,
  COALESCE(CAST(state_region AS STRING), {{ var('nao_inf') }}) AS ust_state_region,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS ust_insert_date
FROM
  {{ ref(
    'brz_erp_us_states'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
