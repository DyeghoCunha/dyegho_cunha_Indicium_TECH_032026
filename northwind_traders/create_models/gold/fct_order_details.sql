CREATE OR REPLACE TABLE fct_order_details (
    sk_order_detail STRING NOT NULL,
    sk_order STRING NOT NULL,
    sk_product STRING NOT NULL,
    nk_order_id INT,
    nk_product_id INT,
    ord_unit_price DECIMAL(10, 2),
    ord_quantity INT,
    ord_discount DECIMAL(10, 2),
    gross_amount DECIMAL(10, 2),
    net_amount DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    gold_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT pk_fct_order_details PRIMARY KEY (sk_order_detail)
  ) USING DELTA TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration' = '7 days',
    'delta.autoOptimize.autoCompact' = 'auto',
    'spark.databricks.delta.autoCompact.enabled' = 'true'
  );