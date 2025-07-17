-- Intermediate model to combine and harmonize audit item data from multiple staging sources.
-- This model applies filters, joins, and unions to create a unified dataset.

{{ config(materialized='view') }}

WITH combined_audit_items AS (
  SELECT
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    buscomp_name,
    field_name,
    operation_cd,
    record_id,
    user_id,
    operation_dt,
    child_bc_name,
    new_val,
    old_val,
    item_iden_num,
    audit_log,
    db_last_upd,
    sync_dt,
    old_owner,
    new_owner,
    old_source,
    new_source,
    old_status,
    new_status
  FROM {{ ref('stg_tbl_s_audit_item_newprocess') }}

  UNION ALL

  SELECT
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    buscomp_name,
    field_name,
    operation_cd,
    record_id,
    user_id,
    operation_dt,
    child_bc_name,
    new_val,
    old_val,
    item_iden_num,
    audit_log,
    db_last_upd,
    sync_dt,
    old_owner,
    new_owner,
    old_source,
    new_source,
    old_status,
    new_status
  FROM {{ ref('stg_tbl_s_audit_item_sf_newprocess') }}

  UNION ALL

  SELECT
    row_id,
    created,
    created_by,
    last_upd,
    last_upd_by,
    modification_num,
    conflict_id,
    buscomp_name,
    field_name,
    operation_cd,
    record_id,
    user_id,
    operation_dt,
    child_bc_name,
    new_val,
    old_val,
    item_iden_num,
    audit_log,
    db_last_upd,
    sync_dt,
    old_owner,
    new_owner,
    old_source,
    new_source,
    old_status,
    new_status
  FROM {{ ref('stg_tbl_s_audit_item_temp_newprocess') }}
)

SELECT *
FROM combined_audit_items
{% if is_incremental() %}
WHERE operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}