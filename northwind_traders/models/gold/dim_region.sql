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
  {{ dbt_utils.generate_surrogate_key(['reg_region_id']) }} AS sk_region,
  reg_region_id AS nk_region_id,
  reg_region_description,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_region') }}
