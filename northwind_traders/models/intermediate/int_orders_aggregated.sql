{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'orders'],
    cluster_by=['sk_order', 'sk_customer'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'true',
        'delta.autoOptimize.optimizeWrite': 'true'
    }
) }}

WITH fct_orders AS (
  SELECT
    sk_order,
    sk_customer,
    nk_order_id,
    ord_order_date,
    ord_shipped_date,
    ord_required_date,
    ord_freight,
    ord_ship_country
  FROM
    {{ ref('fct_orders') }}
),

fct_details AS (
  SELECT
    sk_order,
    sk_product,
    net_amount,
    discount_amount,
    odt_quantity
  FROM
    {{ ref('fct_order_details') }}
)

SELECT
  o.sk_order,
  o.sk_customer,
  o.nk_order_id,
  o.ord_order_date,
  o.ord_ship_country,
  o.ord_freight,
  o.ord_shipped_date,
  o.ord_required_date,
  SUM(od.net_amount) AS order_value,
  SUM(od.discount_amount) AS order_discount,
  SUM(od.odt_quantity) AS order_quantity,
  COUNT(DISTINCT od.sk_product) AS products_per_order,
  DATEDIFF(o.ord_shipped_date, o.ord_order_date) AS order_to_ship_days,
  CASE
    WHEN o.ord_shipped_date > o.ord_required_date THEN 1
    ELSE 0
  END AS is_late,
  CASE
    WHEN o.ord_shipped_date > o.ord_required_date THEN DATEDIFF(o.ord_shipped_date, o.ord_required_date)
    ELSE 0
  END AS late_by_days
FROM
  fct_orders o
LEFT JOIN 
  fct_details od ON o.sk_order = od.sk_order
GROUP BY
  o.sk_order,
  o.sk_customer,
  o.nk_order_id,
  o.ord_order_date,
  o.ord_ship_country,
  o.ord_freight,
  o.ord_shipped_date,
  o.ord_required_date