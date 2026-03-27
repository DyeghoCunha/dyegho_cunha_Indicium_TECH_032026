CREATE OR REPLACE TABLE mart_customer_360 (
    sk_customer STRING NOT NULL,
    nk_customer_id STRING,
    ctm_company_name STRING,
    total_revenue DECIMAL(15, 2),
    total_orders INT,
    avg_order_value DECIMAL(10, 2),
    rfm_segment STRING,
    customer_health_score DECIMAL(5, 2),
    is_churned BOOLEAN,
    calculated_at TIMESTAMP,
    CONSTRAINT pk_mart_customer_360 PRIMARY KEY (sk_customer)
  ) USING DELTA TBLPROPERTIES (
    'delta.logRetentionDuration' = '7 days',
    'delta.autoOptimize.autoCompact' = 'auto',
    'spark.databricks.delta.autoCompact.enabled' = 'true'
  );