CREATE
OR REPLACE TABLE fct_customer_demo (
  sk_customer_demo STRING NOT NULL,
  sk_customer STRING NOT NULL,
  sk_customer_demographic STRING NOT NULL,
  nk_customer_id STRING,
  nk_customer_type_id STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_fct_customer_demo PRIMARY KEY (sk_customer_demo)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
