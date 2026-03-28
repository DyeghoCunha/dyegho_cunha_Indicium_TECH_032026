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
  {{ dbt_utils.generate_surrogate_key(['ter_territory_id']) }} AS sk_territory,
  {{ dbt_utils.generate_surrogate_key(['reg_region_id']) }} AS sk_region,
  ter_territory_id AS nk_territory_id,
  ter_territory_description,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_territories') }}
