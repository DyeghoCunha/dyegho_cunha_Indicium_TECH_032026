CREATE OR REPLACE TABLE dim_customers (
  sk_customer STRING NOT NULL COMMENT 'Surrogate Key (Hash MD5)',
  nk_customer_id STRING NOT NULL COMMENT 'Natural Key original do ERP',
  ctm_company_name STRING,
  ctm_contact_name STRING,
  ctm_contact_title STRING,
  ctm_address STRING,
  ctm_city STRING,
  ctm_region STRING,
  ctm_postal_code STRING,
  ctm_country STRING,
  ctm_phone STRING,
  ctm_fax STRING,
  silver_insert_date TIMESTAMP,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_customers PRIMARY KEY (sk_customer)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_customers (
    sk_customer,
    nk_customer_id,
    ctm_company_name,
    ctm_contact_name,
    ctm_contact_title,
    ctm_address,
    ctm_city,
    ctm_region,
    ctm_postal_code,
    ctm_country,
    ctm_phone,
    ctm_fax
  )
VALUES
  (MD5('-1'), '-1', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado');
