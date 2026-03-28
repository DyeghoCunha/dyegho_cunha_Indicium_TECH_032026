{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'core'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'auto',
        'spark.databricks.delta.autoCompact.enabled': 'true'
    }
) }}

SELECT

    {{ dbt_utils.generate_surrogate_key(['p.prd_product_id']) }} AS sk_product,
    
    p.prd_product_id AS nk_product_id,
    p.sup_supplier_id AS nk_supplier_id,
    p.cat_category_id AS nk_category_id,
    
    p.prd_product_name,
    c.cat_category_name,     
    c.cat_description,       
    s.sup_company_name,      
    s.sup_country,           
    
    p.prd_quantity_per_unit,
    p.prd_unit_price,
    p.prd_units_in_stock,
    p.prd_units_on_order,
    p.prd_reorder_level,
    p.prd_is_discontinued,
    
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_dim_products') }} p
LEFT JOIN {{ ref('pre_dim_categories') }} c ON p.cat_category_id = c.cat_category_id
LEFT JOIN {{ ref('pre_dim_suppliers') }} s ON p.sup_supplier_id = s.sup_supplier_id