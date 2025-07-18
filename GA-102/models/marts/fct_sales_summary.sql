-- Fact model to summarize sales data by region

WITH sales_summary AS (
  SELECT
    region_name,
    SUM(total_amount) AS total_sales
  FROM
    {{ ref('int_sales_data') }}
  WHERE
    total_amount >= 0
  GROUP BY
    region_name
)

SELECT
  region_name,
  total_sales
FROM
  sales_summary
{% if is_incremental() %}
WHERE
  operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}