CREATE
OR REPLACE TABLE dim_us_states (
  sk_us_state STRING NOT NULL,
  nk_state_id INT NOT NULL,
  ust_state_name STRING,
  ust_state_abbr STRING,
  ust_state_region STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_us_states PRIMARY KEY (sk_us_state)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_us_states (
    sk_us_state,
    nk_state_id,
    ust_state_name,
    ust_state_abbr,
    ust_state_region
  )
VALUES
  (MD5('-1'), -1, 'Não Informado', 'N/A', 'Não Informado');
