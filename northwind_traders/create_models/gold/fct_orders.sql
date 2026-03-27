CREATE OR REPLACE TABLE fct_orders (
    sk_order STRING NOT NULL,
    sk_customer STRING,
    sk_employee STRING,
    sk_shipper STRING,
    nk_order_id INT NOT NULL,
    ord_order_date DATE,
    ord_required_date DATE,
    ord_shipped_date DATE,
    ord_freight DECIMAL(10, 2),
    ord_ship_name STRING,
    ord_ship_city STRING,
    ord_ship_region STRING,
    ord_ship_country STRING,
    gold_insert_date TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT pk_fct_orders PRIMARY KEY (sk_order)
  ) USING DELTA TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.logRetentionDuration' = '7 days',
    'delta.autoOptimize.autoCompact' = 'auto',
    'spark.databricks.delta.autoCompact.enabled' = 'true'
  );