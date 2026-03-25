CREATE OR REPLACE TABLE pre_dim_customers (
    ctm_customer_id STRING NOT NULL COMMENT 'Código original do cliente (Natural Key)',
    ctm_company_name STRING COMMENT 'Razão Social / Nome da Empresa',
    ctm_contact_name STRING COMMENT 'Nome do contato principal',
    ctm_contact_title STRING COMMENT 'Cargo do contato',
    ctm_address STRING COMMENT 'Endereço',
    ctm_city STRING COMMENT 'Cidade',
    ctm_region STRING COMMENT 'Região/Estado',
    ctm_postal_code STRING COMMENT 'Código Postal / CEP',
    ctm_country STRING COMMENT 'País',
    ctm_phone STRING COMMENT 'Telefone',
    ctm_fax STRING COMMENT 'Fax',
    ctm_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_customers PRIMARY KEY (ctm_customer_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);
COMMENT ON TABLE pre_dim_customers IS 'Dimensão de Clientes: Contém os dados cadastrais, localização e contatos dos clientes B2B da Northwind.';