{{ config(materialized='incremental', unique_key='row_id') }}

-- Fact table for audit items with incremental updates
WITH audit_fact AS (
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
    audit.updated_old_status,
    audit.updated_new_status
  FROM {{ ref('int_audit_item_transformed') }} audit
)

SELECT *
FROM audit_fact
{% if is_incremental() %}
WHERE audit_fact.operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}