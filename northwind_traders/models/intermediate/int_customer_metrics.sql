{{ config(
    materialized='table',
    schema='intermediate', 
    tags=['intermediate', 'customer']
) }}

WITH order_aggregates AS (
  SELECT * FROM {{ ref('int_orders_aggregated') }}
),

customer_base AS (
  SELECT 
    sk_customer,
    MIN(ord_order_date) AS first_order_date,
    MAX(ord_order_date) AS last_order_date,
    DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), MAX(ord_order_date)) AS recency_days,
    DATEDIFF(MAX(ord_order_date), MIN(ord_order_date)) AS customer_tenure_days,
    COUNT(DISTINCT sk_order) AS total_orders,
    SUM(order_value) AS total_revenue,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 30 THEN order_value ELSE 0 END) AS revenue_last_30d,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 90 THEN order_value ELSE 0 END) AS revenue_last_90d,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 365 THEN order_value ELSE 0 END) AS revenue_last_365d,
    AVG(order_value) AS avg_order_value,
    MIN(order_value) AS min_order_value,
    MAX(order_value) AS max_order_value,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 30 THEN 1 ELSE 0 END) AS orders_last_30d,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 90 THEN 1 ELSE 0 END) AS orders_last_90d,
    SUM(CASE WHEN DATEDIFF(CAST('{{ var("current_business_date", "1998-05-31") }}' AS DATE), ord_order_date) <= 365 THEN 1 ELSE 0 END) AS orders_last_365d,
    SUM(order_quantity) AS total_quantity,
    AVG(order_quantity) AS avg_quantity_per_order,
    SUM(products_per_order) AS total_products_purchased,
    AVG(products_per_order) AS avg_products_per_order,
    SUM(order_discount) AS total_discounts_received,
    AVG(order_discount / NULLIF(order_value + order_discount, 0)) AS avg_discount_pct,
    SUM(ord_freight) AS total_freight_paid,
    AVG(ord_freight) AS avg_freight_per_order,
    AVG(ord_freight / NULLIF(order_value, 0)) AS avg_freight_pct_of_order
  FROM order_aggregates
  GROUP BY sk_customer
),

customer_behavior AS (
  SELECT 
    sk_customer,
    AVG(days_between_orders) AS inter_purchase_interval_days,
    STDDEV(days_between_orders) AS ipi_stddev,
    AVG(order_to_ship_days) AS avg_order_to_ship_days,
    SUM(is_late) AS late_deliveries,
    AVG(late_by_days) AS avg_late_by_days
  FROM (
    SELECT 
      sk_customer,
      order_to_ship_days,
      is_late,
      late_by_days,
      DATEDIFF(
        ord_order_date,
        LAG(ord_order_date) OVER (PARTITION BY sk_customer ORDER BY ord_order_date)
      ) AS days_between_orders
    FROM order_aggregates
  ) t
  GROUP BY sk_customer
)

SELECT 
  cb.*,
  beh.inter_purchase_interval_days,
  beh.ipi_stddev,
  beh.avg_order_to_ship_days,
  beh.late_deliveries,
  beh.avg_late_by_days
FROM customer_base cb
LEFT JOIN customer_behavior beh ON cb.sk_customer = beh.sk_customer