-- Intermediate model to handle status-related transformations, including updates to Old_Status and New_Status.

{{ config(materialized='incremental', unique_key='row_id', incremental_strategy='merge') }}

WITH status_updates AS (
  SELECT
    row_id,
    operation_dt,
    old_status,
    new_status,
    CASE 
      WHEN old_status = 'Re-Open' THEN 'Reopen_SF'
      WHEN old_status = 'Customer/Agent Action Needed' THEN 'Pending Documentation'
      ELSE old_status
    END AS updated_old_status,
    CASE 
      WHEN new_status = 'Closed' THEN 'Resolved'
      WHEN new_status = 'Open' THEN 'In Progress'
      ELSE new_status
    END AS updated_new_status
  FROM {{ ref('stg_tbl_s_audit_item_sf_newprocess') }}
)
SELECT
  row_id,
  operation_dt,
  updated_old_status,
  updated_new_status
FROM status_updates
{% if is_incremental() %}
WHERE operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}