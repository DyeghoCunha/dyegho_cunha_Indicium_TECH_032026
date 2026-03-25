CREATE OR REPLACE TABLE pre_dim_employees (
    emp_employee_id INT NOT NULL COMMENT 'Código original do funcionário (Natural Key)',
    emp_last_name STRING COMMENT 'Sobrenome',
    emp_first_name STRING COMMENT 'Nome',
    emp_title STRING COMMENT 'Cargo',
    emp_title_of_courtesy STRING COMMENT 'Pronome de tratamento',
    emp_birth_date DATE COMMENT 'Data de nascimento',
    emp_hire_date DATE COMMENT 'Data de contratação',
    emp_address STRING COMMENT 'Endereço',
    emp_city STRING COMMENT 'Cidade',
    emp_region STRING COMMENT 'Região',
    emp_postal_code STRING COMMENT 'Código Postal',
    emp_country STRING COMMENT 'País',
    emp_home_phone STRING COMMENT 'Telefone residencial',
    emp_extension STRING COMMENT 'Ramal interno',
    emp_reports_to INT COMMENT 'FK para o gerente (Employee ID)',
    emp_notes STRING COMMENT 'Anotações gerais do RH',
    emp_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_employees PRIMARY KEY (emp_employee_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);

COMMENT ON TABLE pre_dim_employees IS 'Dimensão de Funcionários: Hierarquia de vendas e dados demográficos do time interno da Northwind.';