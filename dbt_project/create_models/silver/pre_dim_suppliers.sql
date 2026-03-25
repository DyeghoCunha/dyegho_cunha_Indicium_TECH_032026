CREATE OR REPLACE TABLE pre_dim_suppliers (
    sup_supplier_id INT NOT NULL COMMENT 'Código original do fornecedor (Natural Key)',
    sup_company_name STRING COMMENT 'Razão Social / Nome da Empresa',
    sup_contact_name STRING COMMENT 'Nome do contato principal',
    sup_contact_title STRING COMMENT 'Cargo do contato',
    sup_address STRING COMMENT 'Endereço',
    sup_city STRING COMMENT 'Cidade',
    sup_region STRING COMMENT 'Região/Estado',
    sup_postal_code STRING COMMENT 'Código Postal / CEP',
    sup_country STRING COMMENT 'País',
    sup_phone STRING COMMENT 'Telefone',
    sup_fax STRING COMMENT 'Fax',
    sup_homepage STRING COMMENT 'Site do fornecedor',
    sup_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_suppliers PRIMARY KEY (sup_supplier_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);