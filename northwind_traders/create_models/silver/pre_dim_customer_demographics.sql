CREATE OR REPLACE TABLE pre_dim_customer_demographics (
    cdm_customer_type_id STRING NOT NULL COMMENT 'ID do tipo demográfico (Natural Key)',
    cdm_customer_desc STRING COMMENT 'Descrição do perfil demográfico',
    cdm_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_customer_demographics PRIMARY KEY (cdm_customer_type_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);

COMMENT ON TABLE pre_dim_customer_demographics IS 'Dimensão de Demografia: Contém as descrições dos perfis demográficos para segmentação de clientes.';