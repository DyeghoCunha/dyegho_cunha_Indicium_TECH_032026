{{ config(
  materialized = 'table',
  schema = 'gold',
  tags = ['gold', 'core', 'fact']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['ctm_customer_id', 'cdm_customer_type_id']) }} AS sk_customer_demo,
  {{ dbt_utils.generate_surrogate_key(['ctm_customer_id']) }} AS sk_customer,
  {{ dbt_utils.generate_surrogate_key(['cdm_customer_type_id']) }} AS sk_customer_demographic,
  ctm_customer_id AS nk_customer_id,
  cdm_customer_type_id AS nk_customer_type_id,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_fct_customer_customer_demo') }}
