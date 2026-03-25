CREATE OR REPLACE TABLE pre_dim_us_states (
    ust_state_id INT NOT NULL COMMENT 'ID original do estado (Natural Key)',
    ust_state_name STRING COMMENT 'Nome por extenso do estado',
    ust_state_abbr STRING COMMENT 'Sigla/Abreviação (ex: NY, CA)',
    ust_state_region STRING COMMENT 'Região geográfica específica do estado',
    ust_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_us_states PRIMARY KEY (ust_state_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);

COMMENT ON TABLE pre_dim_us_states IS 'Dimensão de Estados (EUA): Cadastro auxiliar para normalização de endereços e análises regionais nos EUA.';