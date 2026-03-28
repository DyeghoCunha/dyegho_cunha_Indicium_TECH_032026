{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_employee_territory',
    schema = 'gold',
    tags = ['gold', 'core', 'fact', 'bridge'],
    tblproperties ={ 'delta.logRetentionDuration': '7 days',
    'delta.autoOptimize.autoCompact': 'auto',
    'delta.autoOptimize.optimizeWrite': 'true' }
) }}

WITH source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['emp_employee_id', 'ter_territory_id']) }} AS sk_employee_territory,
        {{ dbt_utils.generate_surrogate_key(['emp_employee_id']) }} AS sk_employee,
        {{ dbt_utils.generate_surrogate_key(['ter_territory_id']) }} AS sk_territory,
        emp_employee_id AS nk_employee_id,
        ter_territory_id AS nk_territory_id,
        bronze_insert_date,
        from_utc_timestamp(now(), 'GMT-3') AS gold_insert_date
    FROM
        {{ ref('pre_fct_employee_territories') }}

{% if is_incremental() %}
WHERE
    bronze_insert_date > (
        SELECT
            COALESCE(MAX(gold_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
        FROM
            {{ this }})
        {% endif %}
    )

SELECT
    sk_employee_territory,
    sk_employee,
    sk_territory,
    nk_employee_id,
    nk_territory_id,
    bronze_insert_date,
    gold_insert_date
FROM
    source
