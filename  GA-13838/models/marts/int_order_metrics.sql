-- This model calculates order-level metrics including total payment amount and payment method breakdown.

WITH order_payments AS (
  SELECT
    o."id" AS "order_id",
    SUM(p."amount") AS "total_amount",
    SUM(CASE WHEN p."payment_method" = 'credit_card' THEN p."amount" ELSE 0 END) AS "credit_card_amount",
    SUM(CASE WHEN p."payment_method" = 'coupon' THEN p."amount" ELSE 0 END) AS "coupon_amount",
    SUM(CASE WHEN p."payment_method" = 'bank_transfer' THEN p."amount" ELSE 0 END) AS "bank_transfer_amount",
    SUM(CASE WHEN p."payment_method" = 'gift_card' THEN p."amount" ELSE 0 END) AS "gift_card_amount"
  FROM {{ ref('stg_orders') }} o
  LEFT JOIN {{ ref('stg_payments') }} p ON o."id" = p."order_id"
  GROUP BY o."id"
)

SELECT
  "order_id",
  "total_amount",
  "credit_card_amount",
  "coupon_amount",
  "bank_transfer_amount",
  "gift_card_amount"
FROM order_payments