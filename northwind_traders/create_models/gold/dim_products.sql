CREATE
OR REPLACE TABLE dim_products (
  sk_product STRING NOT NULL,
  nk_product_id INT NOT NULL,
  nk_supplier_id INT,
  nk_category_id INT,
  prd_product_name STRING,
  cat_category_name STRING,
  cat_description STRING,
  sup_company_name STRING,
  sup_country STRING,
  prd_quantity_per_unit STRING,
  prd_unit_price DECIMAL(
    10,
    2
  ),
  prd_units_in_stock INT,
  prd_units_on_order INT,
  prd_reorder_level INT,
  prd_is_discontinued BOOLEAN,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_products PRIMARY KEY (sk_product)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_products (
    sk_product,
    nk_product_id,
    nk_supplier_id,
    nk_category_id,
    prd_product_name,
    cat_category_name,
    cat_description,
    sup_company_name,
    sup_country,
    prd_unit_price,
    prd_is_discontinued
  )
VALUES
  (MD5('-1'), -1, -1, -1, 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', 0.00, FALSE);
