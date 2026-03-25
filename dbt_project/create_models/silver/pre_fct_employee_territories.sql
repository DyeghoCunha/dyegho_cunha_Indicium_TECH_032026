CREATE OR REPLACE TABLE pre_fct_employee_territories (
    emp_employee_id INT NOT NULL COMMENT 'FK para Funcionários',
    ter_territory_id STRING NOT NULL COMMENT 'FK para Territórios',
    etr_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_fct_employee_territories PRIMARY KEY (emp_employee_id, ter_territory_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);