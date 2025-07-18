-- Intermediate model for consolidating parsed log transformations

{{ config(materialized='incremental', unique_key='row_id') }}

WITH parsed_logs AS (
  SELECT
    row_id,
    operation_dt,
    fieldname,
    newvalue,
    oldvalue,
    colcode
  FROM {{ ref('stg_tbl_s_audit_item_parsed_log_sf_newprocess') }}
  UNION ALL
  SELECT
    row_id,
    operation_dt,
    fieldname,
    newvalue,
    oldvalue,
    colcode
  FROM {{ ref('stg_tbl_s_audit_item_parsed_log_newprocess') }}
)
SELECT *
FROM parsed_logs
{% if is_incremental() %}
WHERE operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}