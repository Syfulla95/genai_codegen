-- Intermediate model to transform audit item data with LDAP stamping, group name stamping, and status modifications.

{{ config(materialized='incremental') }}

WITH user_lookup AS (
  SELECT
    src_usr_id,
    ldap_id
  FROM {{ ref('stg_sf_ods_srm_usr') }}
  WHERE trans_eff_tmsp <= CURRENT_TIMESTAMP AND trans_exp_tmsp > CURRENT_TIMESTAMP
),
group_lookup AS (
  SELECT
    src_grp_id,
    devlpr_nm
  FROM {{ ref('stg_sf_ods_srm_grp') }}
  WHERE trans_eff_tmsp <= CURRENT_TIMESTAMP AND trans_exp_tmsp > CURRENT_TIMESTAMP
),
audit_item_transformed AS (
  SELECT
    a.row_id,
    a.created,
    a.created_by,
    a.last_upd,
    a.last_upd_by,
    a.modification_num,
    a.conflict_id,
    a.buscomp_name,
    a.field_name,
    a.operation_cd,
    a.record_id,
    a.user_id,
    a.operation_dt,
    a.child_bc_name,
    a.new_val,
    a.old_val,
    a.item_iden_num,
    a.audit_log,
    a.db_last_upd,
    a.sync_dt,
    COALESCE(u.ldap_id, a.old_owner) AS old_owner_ldap,
    COALESCE(u.ldap_id, a.new_owner) AS new_owner_ldap,
    COALESCE(g.devlpr_nm, a.old_source) AS old_source_group,
    COALESCE(g.devlpr_nm, a.new_source) AS new_source_group,
    CASE 
      WHEN a.old_status = 'Re-Open' THEN 'Reopen_SF'
      ELSE a.old_status
    END AS old_status_transformed,
    CASE 
      WHEN a.new_status = 'Re-Open' THEN 'Reopen_SF'
      ELSE a.new_status
    END AS new_status_transformed
  FROM {{ ref('stg_tbl_s_audit_item_sf_newprocess') }} a
  LEFT JOIN user_lookup u ON a.old_owner = u.src_usr_id OR a.new_owner = u.src_usr_id
  LEFT JOIN group_lookup g ON a.old_source = g.src_grp_id OR a.new_source = g.src_grp_id
  {% if is_incremental() %}
  WHERE a.operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
  {% endif %}
)

SELECT *
FROM audit_item_transformed