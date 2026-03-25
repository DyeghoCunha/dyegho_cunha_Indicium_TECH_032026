{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'ter_territory_id',
  tags = ['silver', 'dimensao']
) }}

SELECT
  COALESCE(CAST(territory_id AS STRING), {{ var('nao_inf') }}) AS ter_territory_id,
  COALESCE(CAST(territory_description AS STRING), {{ var('nao_inf') }}) AS ter_territory_description,
  COALESCE(CAST(region_id AS INT), -1) AS reg_region_id,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS ter_insert_date
FROM
  {{ ref(
    'brz_erp_territories'
  ) }}

{% if is_incremental() %}
WHERE
  _insert_date > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
