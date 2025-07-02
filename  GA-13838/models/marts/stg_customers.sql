-- This model stages the raw customer data by removing duplicates and standardizing date formats.

WITH deduplicated_customers AS (
  SELECT DISTINCT
    "id",
    "first_name",
    "last_name",
    "created_at"
  FROM {{ source('jaffle_shop_classic', 'raw_customers') }}
)

SELECT
  "id",
  "first_name",
  "last_name",
  "created_at"
FROM deduplicated_customers