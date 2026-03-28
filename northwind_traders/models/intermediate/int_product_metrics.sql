{{ config(
  materialized = 'table',
  schema = 'intermediate',
  tags = ['intermediate', 'product']
) }}

WITH fct_details AS (

  SELECT
    sk_order_detail,
    sk_order,
    sk_product,
    nk_order_id,
    nk_product_id,
    odt_unit_price,
    odt_quantity,
    odt_discount,
    gross_amount,
    net_amount,
    discount_amount,
    bronze_insert_date,
    gold_insert_date
  FROM
    {{ ref('fct_order_details') }}
),
fct_orders AS (
  SELECT
    sk_order,
    sk_customer,
    sk_employee,
    sk_shipper,
    sk_order_date,
    sk_required_date,
    sk_shipped_date,
    nk_order_id,
    ord_order_date,
    ord_freight,
    ord_ship_name,
    ord_ship_city,
    ord_ship_region,
    ord_ship_country,
    bronze_insert_date,
    gold_insert_date
  FROM
    {{ ref('fct_orders') }}
),
product_sales AS (
  SELECT
    od.sk_product,
    o.ord_order_date,
    o.sk_customer,
    od.odt_unit_price,
    od.odt_quantity,
    od.odt_discount,
    od.net_amount
  FROM
    fct_details od
    INNER JOIN fct_orders o
    ON od.sk_order = o.sk_order
)
SELECT
  sk_product,
  COUNT(
    DISTINCT ord_order_date
  ) AS days_with_sales,
  SUM(odt_quantity) AS total_quantity_sold,
  SUM(net_amount) AS total_revenue,
  COUNT(
    DISTINCT sk_customer
  ) AS unique_customers,
  MIN(ord_order_date) AS first_sale_date,
  MAX(ord_order_date) AS last_sale_date,
  DATEDIFF(
    CAST(
      '{{ var("current_business_date", "1998-05-31") }}' AS DATE
    ),
    MAX(ord_order_date)
  ) AS days_since_last_sale,
  AVG(odt_unit_price) AS avg_selling_price,
  AVG(odt_discount) AS avg_discount_pct,
  SUM(
    CASE
      WHEN DATEDIFF(
        CAST(
          '{{ var("current_business_date", "1998-05-31") }}' AS DATE
        ),
        ord_order_date
      ) <= 30 THEN odt_quantity
      ELSE 0
    END
  ) AS quantity_sold_last_30d,
  SUM(
    CASE
      WHEN DATEDIFF(
        CAST(
          '{{ var("current_business_date", "1998-05-31") }}' AS DATE
        ),
        ord_order_date
      ) <= 90 THEN odt_quantity
      ELSE 0
    END
  ) AS quantity_sold_last_90d,
  SUM(
    CASE
      WHEN DATEDIFF(
        CAST(
          '{{ var("current_business_date", "1998-05-31") }}' AS DATE
        ),
        ord_order_date
      ) <= 30 THEN net_amount
      ELSE 0
    END
  ) AS revenue_last_30d,
  SUM(
    CASE
      WHEN DATEDIFF(
        CAST(
          '{{ var("current_business_date", "1998-05-31") }}' AS DATE
        ),
        ord_order_date
      ) <= 90 THEN net_amount
      ELSE 0
    END
  ) AS revenue_last_90d
FROM
  product_sales
GROUP BY
  sk_product
