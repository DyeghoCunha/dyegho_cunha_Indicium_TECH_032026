{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'core'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'auto',
        'spark.databricks.delta.autoCompact.enabled': 'true'
    }
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['ust_state_id']) }} AS sk_us_state,
  ust_state_id AS nk_state_id,
  ust_state_name,
  ust_state_abbr,
  ust_state_region,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_us_states') }}
