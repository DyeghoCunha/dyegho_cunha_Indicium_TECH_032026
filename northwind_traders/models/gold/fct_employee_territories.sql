{{ config(materialized='table', schema='gold', tags=['gold', 'core', 'fact']) }}

SELECT
    -- PK Composta gerada via Hash
    {{ dbt_utils.generate_surrogate_key(['emp_employee_id', 'ter_territory_id']) }} AS sk_employee_territory,
    
    -- FKs para as dimensões
    {{ dbt_utils.generate_surrogate_key(['emp_employee_id']) }} AS sk_employee,
    {{ dbt_utils.generate_surrogate_key(['ter_territory_id']) }} AS sk_territory,
    
    -- Natural Keys (opcional, bom para debug)
    emp_employee_id AS nk_employee_id,
    ter_territory_id AS nk_territory_id,
    
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_fct_employee_territories') }}