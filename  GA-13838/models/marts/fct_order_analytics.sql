-- This model provides order analytics including detailed payment breakdown and total order revenue.

SELECT
  o."id" AS "order_id",
  o."user_id" AS "customer_id",
  o."order_date",
  o."status",
  om."total_amount",
  om."credit_card_amount",
  om."coupon_amount",
  om."bank_transfer_amount",
  om."gift_card_amount"
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('int_order_metrics') }} om ON o."id" = om."order_id"