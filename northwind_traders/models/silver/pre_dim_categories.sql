{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'cat_category_id',
    tags = ['silver', 'dimensao']
) }}

WITH source_data AS (
    SELECT 
        COALESCE(CAST(category_id AS INT), -1) AS cat_category_id,
        COALESCE(CAST(category_name AS STRING), {{ var('nao_inf') }}) AS cat_category_name,
        COALESCE(CAST(description AS STRING), {{ var('nao_inf') }})   AS cat_description,
        COALESCE(CAST(picture AS STRING), {{ var('nao_inf') }})       AS cat_picture,
        
        CAST(_source_file AS STRING)        AS cat_source_file,
        CAST(_system_source AS STRING)      AS cat_system_source,
        CAST(_insert_date AS TIMESTAMP)     AS cat_bronze_insert_date,
        
        from_utc_timestamp(now(), 'GMT-3')  AS cat_insert_date
        
    FROM {{ source('erp_northwind', 'categories') }}
)

SELECT * FROM source_data

{% if is_incremental() %}
    WHERE cat_bronze_insert_date > (SELECT COALESCE(MAX(cat_bronze_insert_date), '1900-01-01') FROM {{ this }})
{% endif %}