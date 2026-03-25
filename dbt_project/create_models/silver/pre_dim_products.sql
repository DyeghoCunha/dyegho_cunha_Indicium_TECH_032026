CREATE OR REPLACE TABLE pre_dim_products (
    prd_product_id INT NOT NULL COMMENT 'Código original do produto (Natural Key)',
    prd_product_name STRING COMMENT 'Nome do produto',
    sup_supplier_id INT COMMENT 'FK para a tabela de fornecedores',
    cat_category_id INT COMMENT 'FK para a tabela de categorias',
    prd_quantity_per_unit STRING COMMENT 'Descrição de quantidade por embalagem',
    prd_unit_price DECIMAL(10,2) COMMENT 'Preço unitário base',
    prd_units_in_stock INT COMMENT 'Unidades atualmente em estoque',
    prd_units_on_order INT COMMENT 'Unidades já encomendadas',
    prd_reorder_level INT COMMENT 'Ponto de pedido (estoque mínimo)',
    prd_is_discontinued BOOLEAN COMMENT 'Flag de produto fora de linha',
    prd_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_dim_products PRIMARY KEY (prd_product_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);
COMMENT ON TABLE pre_dim_products IS 'Tabela Silver de Produtos';