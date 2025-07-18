{{ config(materialized='table') }}

-- Intermediate model to apply business logic and transformations
WITH transformed_audit_item AS (
  SELECT
    audit.row_id,
    audit.record_id,
    audit.created_by,
    audit.last_upd_by,
    audit.created,
    audit.last_upd,
    audit.operation_dt,
    audit.modification_num,
    audit.conflict_id,
    audit.buscomp_name,
    audit.user_id,
    audit.old_owner,
    audit.new_owner,
    audit.operation_cd,
    audit.field_name,
    audit.old_user_name,
    audit.new_user_name,
    CASE
      WHEN audit.old_status = 'Re-Open' THEN 'Reopen_SF'
      ELSE audit.old_status
    END AS updated_old_status,
    CASE
      WHEN audit.new_status = 'Closed' THEN 'Closed_SF'
      ELSE audit.new_status
    END AS updated_new_status
  FROM {{ ref('int_audit_item_user_lookup') }} audit
)

SELECT *
FROM transformed_audit_item