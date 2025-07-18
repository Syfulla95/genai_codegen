-- Fact table for audit items, combining owner, status, and source data.

{{ config(materialized='table') }}

WITH combined_data AS (
  SELECT
    o.row_id,
    o.operation_dt,
    o.old_owner_name,
    o.new_owner_name,
    o.old_owner_group,
    o.new_owner_group,
    s.updated_old_status,
    s.updated_new_status,
    src.updated_old_source,
    src.updated_new_source
  FROM {{ ref('int_audit_item_owner') }} AS o
  LEFT JOIN {{ ref('int_audit_item_status') }} AS s
    ON o.row_id = s.row_id
  LEFT JOIN {{ ref('int_audit_item_source') }} AS src
    ON o.row_id = src.row_id
)
SELECT
  row_id,
  operation_dt,
  old_owner_name,
  new_owner_name,
  old_owner_group,
  new_owner_group,
  updated_old_status,
  updated_new_status,
  updated_old_source,
  updated_new_source
FROM combined_data