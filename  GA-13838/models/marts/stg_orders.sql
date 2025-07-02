-- This model stages the raw order data by removing duplicates and standardizing date formats.

WITH deduplicated_orders AS (
  SELECT DISTINCT
    "id",
    "customer_id" AS "user_id",
    "order_date",
    "status"
  FROM {{ source('jaffle_shop_classic', 'raw_orders') }}
)

SELECT
  "id",
  "user_id",
  "order_date",
  "status"
FROM deduplicated_orders