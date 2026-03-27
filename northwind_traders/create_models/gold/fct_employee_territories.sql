CREATE
OR REPLACE TABLE fct_employee_territories (
  sk_employee_territory STRING NOT NULL,
  sk_employee STRING NOT NULL,
  sk_territory STRING NOT NULL,
  nk_employee_id INT,
  nk_territory_id STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_fct_employee_territories PRIMARY KEY (sk_employee_territory)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
