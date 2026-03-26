{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'emp_employee_id',
    tags = ['silver', 'dimensao']
) }}

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

{% if is_incremental() %}
WHERE
    CAST(
        _insert_date AS TIMESTAMP
    ) > (
        SELECT
            COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
        FROM
            {{ this }})
        {% endif %}
