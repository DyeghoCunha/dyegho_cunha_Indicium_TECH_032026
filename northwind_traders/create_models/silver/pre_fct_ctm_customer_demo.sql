CREATE OR REPLACE TABLE pre_fct_customer_customer_demo (
    ctm_customer_id STRING NOT NULL COMMENT 'FK para Clientes',
    cdm_customer_type_id STRING NOT NULL COMMENT 'FK para Demografia',
    ccd_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_fct_customer_customer_demo PRIMARY KEY (ctm_customer_id, cdm_customer_type_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);

COMMENT ON TABLE pre_fct_customer_customer_demo IS 'Tabela Ponte (Bridge): Relaciona (N:N) os clientes aos seus diversos perfis demográficos.';