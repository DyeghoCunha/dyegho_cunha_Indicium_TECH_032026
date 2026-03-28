{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'dimension']
) }}

SELECT
  COALESCE(CAST(shipper_id AS INT), -1) AS shp_shipper_id,
  COALESCE(CAST(company_name AS STRING), {{ var('nao_inf') }}) AS shp_company_name,
  COALESCE(CAST(phone AS STRING), {{ var('nao_inf') }}) AS shp_phone,
  CAST(
    _insert_date AS TIMESTAMP
  ) AS bronze_insert_date,
  from_utc_timestamp(now(), 'GMT-3') AS shp_insert_date
FROM
  {{ ref(
    'brz_erp_shippers'
  ) }}


