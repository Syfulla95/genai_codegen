-- Final fact table for audit item data, optimized for reporting.

{{ config(materialized='table') }}

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
  old_owner_ldap,
  new_owner_ldap,
  old_source_group,
  new_source_group,
  old_status_transformed,
  new_status_transformed
FROM {{ ref('int_audit_item_transformation') }}