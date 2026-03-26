{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'reg_region_id',
  tags = ['silver', 'dimensao']
) }}

SELECT
  COALESCE(CAST(region_id AS INT), -1) AS reg_region_id,
  COALESCE(CAST(region_description AS STRING), {{ var('nao_inf') }}) AS reg_region_description,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS reg_insert_date
FROM
  {{ ref(
    'brz_erp_region'
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
