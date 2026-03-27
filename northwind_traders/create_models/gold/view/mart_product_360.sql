CREATE OR REPLACE TABLE mart_product_360 (
    sk_product STRING NOT NULL,
    nk_product_id INT,
    prd_product_name STRING,
    total_revenue DECIMAL(15, 2),
    total_quantity_sold INT,
    days_of_inventory INT,
    product_lifecycle_stage STRING,
    is_stockout_risk BOOLEAN,
    calculated_at TIMESTAMP,
    CONSTRAINT pk_mart_product_360 PRIMARY KEY (sk_product)
  ) USING DELTA TBLPROPERTIES (
    'delta.logRetentionDuration' = '7 days',
    'delta.autoOptimize.autoCompact' = 'auto',
    'spark.databricks.delta.autoCompact.enabled' = 'true'
  );