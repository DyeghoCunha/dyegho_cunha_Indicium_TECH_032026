{{ config(
  materialized = 'table',
  schema = 'gold',
  tags = ['gold', 'core', 'dimension']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['reg_region_id']) }} AS sk_region,
  reg_region_id AS nk_region_id,
  reg_region_description,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_region') }}
