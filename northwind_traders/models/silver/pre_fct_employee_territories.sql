{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['emp_employee_id', 'ter_territory_id'],
  schema = 'silver',
  tags = ['silver', 'fact'],
  tblproperties ={ 'delta.logRetentionDuration': '7 days',
  'delta.autoOptimize.autoCompact': 'auto',
  'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source AS (

  SELECT
    COALESCE(CAST(employee_id AS INT), -1) AS emp_employee_id,
    COALESCE(CAST(territory_id AS STRING), {{ var('nao_inf') }}) AS ter_territory_id,
    CAST(
      _insert_date AS TIMESTAMP
    ) AS bronze_insert_date,
    from_utc_timestamp(now(), 'GMT-3') AS etr_insert_date
  FROM
    {{ ref('brz_erp_employee_territories') }}

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
  )
SELECT
  emp_employee_id,
  ter_territory_id,
  bronze_insert_date,
  etr_insert_date
FROM
  source
