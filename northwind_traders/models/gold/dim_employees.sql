{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'core'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'auto',
        'delta.autoOptimize.optimizeWrite': 'true'
    }
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('pre_dim_employees') }}
),

ghost_record AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["-1"]) }} AS sk_employee,
        -1 AS nk_employee_id,
        {{ dbt_utils.generate_surrogate_key(["-1"]) }} AS sk_manager,
        {{ var('nao_inf') }} AS emp_last_name,
        {{ var('nao_inf') }} AS emp_first_name,
        {{ var('nao_inf') }} AS emp_full_name,
        {{ var('nao_inf') }} AS emp_title,
        {{ var('nao_inf') }} AS emp_title_of_courtesy,
        CAST({{ var('date_default') }} AS DATE) AS emp_birth_date,
        CAST({{ var('date_default') }} AS DATE) AS emp_hire_date,
        {{ var('nao_inf') }} AS emp_city,
        {{ var('nao_inf') }} AS emp_region,
        {{ var('nao_inf') }} AS emp_country,
        -1 AS nk_manager_id,
        CURRENT_TIMESTAMP() AS gold_insert_date
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
        CONCAT(emp_first_name, ' ', emp_last_name) AS emp_full_name,
        emp_title,
        emp_title_of_courtesy,
        emp_birth_date,
        emp_hire_date,
        emp_city,
        emp_region,
        emp_country,
        COALESCE(emp_reports_to, -1) AS nk_manager_id,
        CURRENT_TIMESTAMP() AS gold_insert_date
    FROM source_data
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
FROM ghost_record

UNION ALL

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
FROM transformed_data