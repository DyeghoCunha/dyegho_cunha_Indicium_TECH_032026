CREATE
OR REPLACE TABLE dim_territories (
  sk_territory STRING NOT NULL,
  nk_territory_id STRING NOT NULL,
  sk_region STRING,
  ter_territory_description STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_territories PRIMARY KEY (sk_territory)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_territories (
    sk_territory,
    nk_territory_id,
    sk_region,
    ter_territory_description
  )
VALUES
  (MD5('-1'), '-1', MD5('-1'), 'Não Informado');
