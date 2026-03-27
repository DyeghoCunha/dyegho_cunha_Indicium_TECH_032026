{{ config(
  materialized = 'table',
  schema = 'gold',
  tags = ['gold', 'core', 'dimension']
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['emp_employee_id']) }} AS sk_employee,
  emp_employee_id AS nk_employee_id,
  {{ dbt_utils.generate_surrogate_key(['emp_reports_to']) }} AS sk_manager,
  emp_last_name,
  emp_first_name,
  CONCAT(
    emp_first_name,
    ' ',
    emp_last_name
  ) AS emp_full_name,
  emp_title,
  emp_title_of_courtesy,
  emp_birth_date,
  emp_hire_date,
  emp_city,
  emp_region,
  emp_country,
  emp_reports_to AS nk_manager_id,
  CURRENT_TIMESTAMP() AS gold_insert_date
FROM
  {{ ref('pre_dim_employees') }}
