CREATE
OR REPLACE TABLE dim_employees (
  sk_employee STRING NOT NULL,
  nk_employee_id INT NOT NULL,
  sk_manager STRING,
  emp_last_name STRING,
  emp_first_name STRING,
  emp_full_name STRING,
  emp_title STRING,
  emp_title_of_courtesy STRING,
  emp_birth_date DATE,
  emp_hire_date DATE,
  emp_city STRING,
  emp_region STRING,
  emp_country STRING,
  nk_manager_id INT,
  gold_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_dim_employees PRIMARY KEY (sk_employee)
) USING DELTA TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.logRetentionDuration' = '7 days',
  'delta.autoOptimize.autoCompact' = 'auto',
  'spark.databricks.delta.autoCompact.enabled' = 'true'
);
INSERT INTO
  dim_employees (
    sk_employee,
    nk_employee_id,
    sk_manager,
    emp_last_name,
    emp_first_name,
    emp_full_name,
    emp_title,
    emp_birth_date,
    emp_hire_date,
    nk_manager_id
  )
VALUES
  (MD5('-1'), -1, MD5('-1'), 'Não Informado', 'Não Informado', 'Não Informado', 'Não Informado', '1900-01-01', '1900-01-01', -1);
