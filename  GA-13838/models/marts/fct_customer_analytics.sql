-- This model provides customer analytics including lifetime value and order metrics.

WITH customer_lifetime_value AS (
  SELECT
    cm."customer_id",
    c."first_name",
    c."last_name",
    cm."first_order",
    cm."most_recent_order",
    cm."number_of_orders",
    SUM(op."total_amount") AS "customer_lifetime_value"
  FROM {{ ref('int_customer_metrics') }} cm
  LEFT JOIN {{ ref('stg_customers') }} c ON cm."customer_id" = c."id"
  LEFT JOIN {{ ref('int_order_metrics') }} op ON cm."customer_id" = op."order_id"
  GROUP BY cm."customer_id", c."first_name", c."last_name", cm."first_order", cm."most_recent_order", cm."number_of_orders"
)

SELECT
  "customer_id",
  "first_name",
  "last_name",
  "first_order",
  "most_recent_order",
  "number_of_orders",
  "customer_lifetime_value"
FROM customer_lifetime_value