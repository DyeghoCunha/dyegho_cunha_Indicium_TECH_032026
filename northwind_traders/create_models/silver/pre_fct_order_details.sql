CREATE OR REPLACE TABLE pre_fct_order_details (
    ord_order_id INT NOT NULL COMMENT 'Código do Pedido (FK e PK Composta)',
    prd_product_id INT NOT NULL COMMENT 'Código do Produto (FK e PK Composta)',
    odt_unit_price DECIMAL(10,2) NOT NULL COMMENT 'Preço unitário no momento da venda',
    odt_quantity INT NOT NULL COMMENT 'Quantidade comprada',
    odt_discount DECIMAL(5,3) DEFAULT 0.0 COMMENT 'Desconto aplicado',
    odt_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_fct_order_details PRIMARY KEY (ord_order_id, prd_product_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);
COMMENT ON TABLE pre_fct_order_details IS 'Fato de Itens do Pedido: Registra o faturamento real, descontos e volume vendido por produto. Grão: 1 linha por SKU dentro de um Pedido.';