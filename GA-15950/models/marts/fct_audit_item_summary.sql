-- Final fact model to summarize audit item data for business reporting.
-- This model aggregates data and calculates derived metrics.

{{ config(materialized='table') }}

WITH audit_item_summary AS (
  SELECT
    user_id,
    COUNT(row_id) AS total_audit_items,
    COUNT(DISTINCT record_id) AS unique_records,
    MAX(operation_dt) AS last_operation_date,
    MIN(operation_dt) AS first_operation_date
  FROM {{ ref('int_audit_item_combined') }}
  GROUP BY user_id
)

SELECT *
FROM audit_item_summary