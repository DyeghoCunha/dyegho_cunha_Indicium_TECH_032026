{{ config(
    materialized = 'table',
    schema = 'gold',
    tags = ['gold', 'dimension', 'core'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source_data AS (

    SELECT
        emp_employee_id,
        emp_last_name,
        emp_first_name,
        emp_title,
        emp_title_of_courtesy,
        emp_birth_date,
        emp_hire_date,
        emp_address,
        emp_city,
        emp_region,
        emp_postal_code,
        emp_country,
        emp_home_phone,
        emp_extension,
        emp_reports_to,
        emp_notes,
        bronze_insert_date,
        emp_insert_date
    FROM
        {{ ref('pre_dim_employees') }}
),
transformed_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['emp_employee_id']) }} AS sk_employee,
        emp_employee_id AS nk_employee_id,
        CASE
            WHEN emp_reports_to IS NULL THEN {{ dbt_utils.generate_surrogate_key(["-1"]) }}
            ELSE {{ dbt_utils.generate_surrogate_key(['emp_reports_to']) }}
        END AS sk_manager,
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
        COALESCE(
            emp_reports_to,
            -1
        ) AS nk_manager_id,
        CURRENT_TIMESTAMP() AS gold_insert_date
    FROM
        source_data
)

SELECT
    sk_employee,
    nk_employee_id,
    sk_manager,
    emp_last_name,
    emp_first_name,
    emp_full_name,
    emp_title,
    emp_title_of_courtesy,
    emp_birth_date,
    emp_hire_date,
    emp_city,
    emp_region,
    emp_country,
    nk_manager_id,
    gold_insert_date
FROM
    transformed_data
