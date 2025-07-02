-- This model stages the raw payment data by removing duplicates and standardizing payment method names.

WITH deduplicated_payments AS (
  SELECT DISTINCT
    "order_id",
    "payment_method",
    "amount"
  FROM {{ source('jaffle_shop_classic', 'raw_payments') }}
)

SELECT
  "order_id",
  "payment_method",
  "amount"
FROM deduplicated_payments