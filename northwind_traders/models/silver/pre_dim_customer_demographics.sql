{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'dimension']
) }}

WITH unique_segments AS (

  SELECT
    customer_segment AS cdm_customer_type_id,
    segment_description AS cdm_customer_desc,
    MAX(CAST(calculated_at AS TIMESTAMP)) AS last_calculated_at
  FROM
    {{ ref('int_customer_segments') }}
  GROUP BY
    cdm_customer_type_id,
    cdm_customer_desc
)
SELECT
  COALESCE(CAST(cdm_customer_type_id AS STRING), {{ var('nao_inf') }}) AS cdm_customer_type_id,
  COALESCE(CAST(cdm_customer_desc AS STRING), {{ var('nao_inf') }}) AS cdm_customer_desc,
  last_calculated_at AS bronze_insert_date, 
  from_utc_timestamp(now(), 'GMT-3') AS cdm_insert_date
FROM
  unique_segments

