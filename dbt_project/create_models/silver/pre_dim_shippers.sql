CREATE OR REPLACE TABLE pre_dim_shippers (
    shp_shipper_id INT NOT NULL COMMENT 'Código original da transportadora (Natural Key)',
    shp_company_name STRING COMMENT 'Nome da transportadora',
    shp_phone STRING COMMENT 'Telefone de contato',
    shp_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_shippers PRIMARY KEY (shp_shipper_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);