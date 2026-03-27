{{ config(
  materialized = 'table',
  schema = 'gold',
  tags = ['gold', 'core', 'dimension']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['cdm_customer_type_id']) }} AS sk_customer_demographic,
  cdm_customer_type_id AS nk_customer_type_id,
  cdm_customer_desc,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_customer_demographics') }}
