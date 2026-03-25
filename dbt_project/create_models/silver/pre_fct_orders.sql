CREATE OR REPLACE TABLE pre_fct_orders (
    ord_order_id INT NOT NULL COMMENT 'Código do Pedido (Natural Key)',
    ctm_customer_id STRING COMMENT 'FK para Clientes',
    emp_employee_id INT COMMENT 'FK para Funcionários',
    shp_shipper_id INT COMMENT 'FK para Transportadoras (ship_via)',
    ord_order_date DATE COMMENT 'Data de realização do pedido',
    ord_required_date DATE COMMENT 'Data limite acordada para entrega',
    ord_shipped_date DATE COMMENT 'Data real de despacho',
    ord_freight DECIMAL(10,2) COMMENT 'Valor do frete',
    ord_ship_name STRING COMMENT 'Nome do destinatário no local de entrega',
    ord_ship_address STRING COMMENT 'Endereço de entrega',
    ord_ship_city STRING COMMENT 'Cidade de entrega',
    ord_ship_region STRING COMMENT 'Estado de entrega',
    ord_ship_postal_code STRING COMMENT 'CEP de entrega',
    ord_ship_country STRING COMMENT 'País de entrega',
    ord_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Auditoria: Data/Hora de atualização na Silver',
    CONSTRAINT pk_pre_fct_orders PRIMARY KEY (ord_order_id)
) USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration'='7 days',
    'delta.autoOptimize.autoCompact'='auto',
    'spark.databricks.delta.autoCompact.enabled'='true'
);
COMMENT ON TABLE pre_fct_orders IS 'Fato de Pedidos (Cabeçalho): Registra o evento de compra, datas de SLA (promessa vs entrega) e custos de frete. Grão: 1 linha por Pedido.';