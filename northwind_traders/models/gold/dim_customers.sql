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
  {{ dbt_utils.generate_surrogate_key(['ctm_customer_id']) }} AS sk_customer,
  ctm_customer_id AS nk_customer_id,
  ctm_company_name,
  ctm_contact_name,
  ctm_contact_title,
  ctm_address,
  ctm_city,
  ctm_region,
  ctm_postal_code,
  ctm_country,
  ctm_phone,
  ctm_fax,
  ctm_insert_date AS silver_insert_date,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_customers') }}
