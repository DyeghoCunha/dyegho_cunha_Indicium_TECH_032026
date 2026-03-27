{{ config(materialized='table', schema='gold', tags=['gold', 'core', 'fact']) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.ord_order_id']) }} AS sk_order,
    
    {{ dbt_utils.generate_surrogate_key(['o.ctm_customer_id']) }} AS sk_customer,
    {{ dbt_utils.generate_surrogate_key(['o.emp_employee_id']) }} AS sk_employee,
    {{ dbt_utils.generate_surrogate_key(['o.shp_shipper_id']) }} AS sk_shipper,
    
    o.ord_order_id AS nk_order_id,
    
    o.ord_order_date,
    o.ord_required_date,
    o.ord_shipped_date,
    
    o.ord_freight,
    o.ord_ship_name,
    o.ord_ship_city,
    o.ord_ship_region,
    o.ord_ship_country,
    
    CURRENT_TIMESTAMP() AS gold_insert_date
FROM {{ ref('pre_fct_orders') }} o