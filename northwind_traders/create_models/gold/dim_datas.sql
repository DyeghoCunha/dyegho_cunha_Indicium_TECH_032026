CREATE
OR REPLACE TABLE dim_datas (
  sk_data STRING NOT NULL,
  nk_data DATE NOT NULL,
  ano INT,
  mes INT,
  dia INT,
  trimestre INT,
  semestre INT,
  dia_semana STRING,
  flag_feriado BOOLEAN,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_datas PRIMARY KEY (sk_data)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_datas (
    sk_data,
    nk_data,
    ano,
    dia_semana
  )
VALUES
  (MD5('1900-01-01'), '1900-01-01', 1900, 'N/A');
