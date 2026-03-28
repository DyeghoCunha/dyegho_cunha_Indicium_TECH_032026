{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'dimension']
) }}

WITH ghost_record AS (
    SELECT
        -1 AS emp_employee_id,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_last_name,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_first_name,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_title,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_title_of_courtesy,
        CAST({{ var('date_default') }} AS DATE) AS emp_birth_date,
        CAST({{ var('date_default') }} AS DATE) AS emp_hire_date,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_address,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_city,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_region,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_postal_code,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_country,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_home_phone,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_extension,
        -1 AS emp_reports_to,
        CAST({{ var('nao_inf') }} AS STRING) AS emp_notes,
        CAST({{ var('date_default') }} AS TIMESTAMP) AS bronze_insert_date,
        from_utc_timestamp(now(), 'GMT-3') AS emp_insert_date
),

source_data AS (
    SELECT
        COALESCE(CAST(employee_id AS INT), -1) AS emp_employee_id,
        COALESCE(CAST(last_name AS STRING), {{ var('nao_inf') }}) AS emp_last_name,
        COALESCE(CAST(first_name AS STRING), {{ var('nao_inf') }}) AS emp_first_name,
        COALESCE(CAST(title AS STRING), {{ var('nao_inf') }}) AS emp_title,
        COALESCE(CAST(title_of_courtesy AS STRING), {{ var('nao_inf') }}) AS emp_title_of_courtesy,
        COALESCE(CAST(birth_date AS DATE), {{ var('date_default') }}) AS emp_birth_date,
        COALESCE(CAST(hire_date AS DATE), {{ var('date_default') }}) AS emp_hire_date,
        COALESCE(CAST(address AS STRING), {{ var('nao_inf') }}) AS emp_address,
        COALESCE(CAST(city AS STRING), {{ var('nao_inf') }}) AS emp_city,
        COALESCE(CAST(region AS STRING), {{ var('nao_inf') }}) AS emp_region,
        COALESCE(CAST(postal_code AS STRING), {{ var('nao_inf') }}) AS emp_postal_code,
        COALESCE(CAST(country AS STRING), {{ var('nao_inf') }}) AS emp_country,
        COALESCE(CAST(home_phone AS STRING), {{ var('nao_inf') }}) AS emp_home_phone,
        COALESCE(CAST(extension AS STRING), {{ var('nao_inf') }}) AS emp_extension,
        COALESCE(CAST(reports_to AS INT), -1) AS emp_reports_to,
        COALESCE(CAST(notes AS STRING), {{ var('nao_inf') }}) AS emp_notes,
        CAST(
            _insert_date AS TIMESTAMP
        ) AS bronze_insert_date,
        from_utc_timestamp(now(), 'GMT-3') AS emp_insert_date
    FROM
        {{ ref('brz_erp_employees') }}
)

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
FROM ghost_record

UNION ALL

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
FROM source_data