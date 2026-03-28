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
    {{ dbt_utils.generate_surrogate_key(['shp_shipper_id']) }} AS sk_shipper,
    shp_shipper_id AS nk_shipper_id,
    shp_company_name,
    shp_phone,
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_dim_shippers') }}