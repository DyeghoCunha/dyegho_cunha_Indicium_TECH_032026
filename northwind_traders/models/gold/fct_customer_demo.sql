{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = 'sk_customer_demo',
  schema = 'gold',
  tags = ['gold', 'core', 'fact'],
  tblproperties ={ 'delta.logRetentionDuration': '7 days',
  'delta.autoOptimize.autoCompact': 'auto',
  'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['ctm_customer_id', 'cdm_customer_type_id']) }} AS sk_customer_demo,
    {{ dbt_utils.generate_surrogate_key(['ctm_customer_id']) }} AS sk_customer,
    {{ dbt_utils.generate_surrogate_key(['cdm_customer_type_id']) }} AS sk_customer_demographic,
    ctm_customer_id AS nk_customer_id,
    cdm_customer_type_id AS nk_customer_type_id,
    bronze_insert_date,
    from_utc_timestamp(now(), 'GMT-3') AS gold_insert_date
  FROM
    {{ ref('pre_fct_customer_customer_demo') }}

{% if is_incremental() %}
WHERE
  bronze_insert_date > (
    SELECT
      COALESCE(MAX(gold_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
    FROM
      {{ this }})
    {% endif %}
  )
SELECT
  sk_customer_demo,
  sk_customer,
  sk_customer_demographic,
  nk_customer_id,
  nk_customer_type_id,
  bronze_insert_date,
  gold_insert_date
FROM
  source
