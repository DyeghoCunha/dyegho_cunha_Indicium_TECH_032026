CREATE
OR REPLACE TABLE dim_customer_demographics (
  sk_customer_demographic STRING NOT NULL,
  nk_customer_type_id STRING NOT NULL,
  cdm_customer_desc STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_customer_demographics PRIMARY KEY (sk_customer_demographic)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_customer_demographics (
    sk_customer_demographic,
    nk_customer_type_id,
    cdm_customer_desc
  )
VALUES
  (MD5('-1'), '-1', 'Não Informado');
