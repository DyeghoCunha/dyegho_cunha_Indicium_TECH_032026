CREATE OR REPLACE TABLE pre_dim_territories (
    ter_territory_id STRING NOT NULL COMMENT 'Código do território (Pode conter zeros à esquerda)',
    ter_territory_description STRING COMMENT 'Nome do território',
    reg_region_id INT COMMENT 'FK para Região',
    ter_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_territories PRIMARY KEY (ter_territory_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);

COMMENT ON TABLE pre_dim_territories IS 'Dimensão de Territórios: Subdivisões das regiões onde os funcionários atuam.';