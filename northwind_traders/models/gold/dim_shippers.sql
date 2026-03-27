{{ config(materialized='table', schema='gold', tags=['gold', 'core', 'dimension']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['shp_shipper_id']) }} AS sk_shipper,
    shp_shipper_id AS nk_shipper_id,
    shp_company_name,
    shp_phone,
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_dim_shippers') }}