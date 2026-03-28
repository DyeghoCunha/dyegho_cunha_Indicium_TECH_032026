{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['ctm_customer_id', 'cdm_customer_type_id'],
  schema = 'silver',
  tags = ['silver', 'fact'],
  tblproperties ={ 'delta.logRetentionDuration': '7 days',
  'delta.autoOptimize.autoCompact': 'auto',
  'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source AS (

  SELECT
    COALESCE(CAST(customer_id AS STRING), {{ var("nao_inf") }}) AS ctm_customer_id,
    COALESCE(CAST(customer_segment AS STRING), {{ var("nao_inf") }}) AS cdm_customer_type_id,
    CAST(
      calculated_at AS TIMESTAMP
    ) AS bronze_insert_date,
    from_utc_timestamp(now(), 'GMT-3') AS ccd_insert_date
  FROM
    {{ ref('int_customer_segments') }}

{% if is_incremental() %}
WHERE
  CAST(
    calculated_at AS TIMESTAMP
  ) > (
    SELECT
      COALESCE(MAX(bronze_insert_date), CAST({{ var("date_default") }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
  )
SELECT
  ctm_customer_id,
  cdm_customer_type_id,
  bronze_insert_date,
  ccd_insert_date
FROM
  source
