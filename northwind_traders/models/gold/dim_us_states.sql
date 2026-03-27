{{ config(
  materialized = 'table',
  schema = 'gold',
  tags = ['gold', 'core', 'dimension']
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
