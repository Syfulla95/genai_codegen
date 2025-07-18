-- Intermediate model to transform sales data with derived columns and lookups

WITH sales_data AS (
  SELECT
    transaction_id,
    quantity,
    price,
    region_id,
    quantity * price AS total_amount,
    CASE WHEN region = 'North' THEN 'Northern Sales' ELSE 'Other Sales' END AS region_name
  FROM
    {{ ref('sales_transactions') }}
)

SELECT
  sd.transaction_id,
  sd.total_amount,
  rl.region_name
FROM
  sales_data sd
LEFT JOIN
  {{ ref('region_lookup') }} rl ON sd.region_id = rl.region_id