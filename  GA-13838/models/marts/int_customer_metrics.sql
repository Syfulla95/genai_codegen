-- This model calculates customer-level metrics such as first order, most recent order, and number of orders.

WITH customer_orders AS (
  SELECT
    c."id" AS "customer_id",
    MIN(o."order_date") AS "first_order",
    MAX(o."order_date") AS "most_recent_order",
    COUNT(o."id") AS "number_of_orders"
  FROM {{ ref('stg_customers') }} c
  LEFT JOIN {{ ref('stg_orders') }} o ON c."id" = o."user_id"
  GROUP BY c."id"
)

SELECT
  "customer_id",
  "first_order",
  "most_recent_order",
  "number_of_orders"
FROM customer_orders