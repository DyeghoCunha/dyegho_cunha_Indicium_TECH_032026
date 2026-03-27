{{ config(materialized='table', schema='gold', tags=['gold', 'core', 'fact']) }}

SELECT
   
    {{ dbt_utils.generate_surrogate_key(['od.ord_order_id', 'od.prd_product_id']) }} AS sk_order_detail,
    

    {{ dbt_utils.generate_surrogate_key(['od.ord_order_id']) }} AS sk_order,
    {{ dbt_utils.generate_surrogate_key(['od.prd_product_id']) }} AS sk_product,
    

    od.ord_order_id AS nk_order_id,
    od.prd_product_id AS nk_product_id,
    
    od.odt_unit_price,
    od.odt_quantity,
    od.odt_discount,
    
    (od.odt_unit_price * od.odt_quantity) AS gross_amount,
    (od.odt_unit_price * od.odt_quantity * (1 - od.odt_discount)) AS net_amount,
    (od.odt_unit_price * od.odt_quantity * od.odt_discount) AS discount_amount,
    
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_fct_order_details') }} od