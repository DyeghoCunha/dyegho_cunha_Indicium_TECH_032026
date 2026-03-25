CREATE OR REPLACE TABLE pre_dim_region (
    reg_region_id INT NOT NULL COMMENT 'Código da região',
    reg_region_description STRING COMMENT 'Descrição da região (Ex: Eastern, Western)',
    reg_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_region PRIMARY KEY (reg_region_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);