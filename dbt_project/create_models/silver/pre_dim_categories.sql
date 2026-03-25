CREATE OR REPLACE TABLE pre_dim_categories (
    cat_category_id INT NOT NULL COMMENT 'Código original da categoria (Natural Key)',
    cat_category_name STRING COMMENT 'Nome da categoria',
    cat_description STRING COMMENT 'Descrição da categoria',
    cat_picture STRING COMMENT 'Imagem/Foto (Base64/Binary convertido para string)',
    cat_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_categories PRIMARY KEY (cat_category_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);