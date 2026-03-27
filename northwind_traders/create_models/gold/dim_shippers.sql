CREATE
OR REPLACE TABLE dim_shippers (
  sk_shipper STRING NOT NULL,
  nk_shipper_id INT NOT NULL,
  shp_company_name STRING,
  shp_phone STRING,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_shippers PRIMARY KEY (sk_shipper)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_shippers (
    sk_shipper,
    nk_shipper_id,
    shp_company_name,
    shp_phone
  )
VALUES
  (MD5('-1'), -1, 'Não Informado', 'Não Informado');
