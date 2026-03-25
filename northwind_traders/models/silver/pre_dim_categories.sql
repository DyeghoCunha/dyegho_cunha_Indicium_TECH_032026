{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'cat_category_id',
    tags = ['silver', 'dimensao']
) }}

SELECT
    COALESCE(CAST(category_id AS INT), -1) AS cat_category_id,
    COALESCE(CAST(category_name AS STRING), {{ var('nao_inf') }}) AS cat_category_name,
    COALESCE(CAST(description AS STRING), {{ var('nao_inf') }}) AS cat_description,
    COALESCE(CAST(picture AS STRING), {{ var('nao_inf') }}) AS cat_picture,
    CAST(
        _insert_date AS TIMESTAMP
    ) AS bronze_insert_date,
    from_utc_timestamp(now(), 'GMT-3') AS cat_insert_date
FROM
    {{ ref(
        'brz_erp_categories'
    ) }}

{% if is_incremental() %}
WHERE
    _insert_date > (
        SELECT
            COALESCE(MAX(bronze_insert_date), CAST({{ var('date_default') }} AS TIMESTAMP))
        FROM
            {{ this }})
        {% endif %}
