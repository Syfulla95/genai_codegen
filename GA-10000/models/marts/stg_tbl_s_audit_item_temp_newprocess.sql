{{ config(materialized='view') }}

SELECT
    "ROW_ID" AS row_id, -- Unique identifier for the row
    "CREATED" AS created, -- Creation timestamp
    "CREATED_BY" AS created_by, -- User who created the record
    "LAST_UPD" AS last_upd, -- Last update timestamp
    "LAST_UPD_BY" AS last_upd_by, -- User who last updated the record
    "MODIFICATION_NUM" AS modification_num, -- Modification number
    "CONFLICT_ID" AS conflict_id, -- Conflict identifier
    "BUSCOMP_NAME" AS buscomp_name, -- Business component name
    "FIELD_NAME" AS field_name, -- Name of the field
    "OPERATION_CD" AS operation_cd, -- Operation code
    "RECORD_ID" AS record_id, -- Record identifier
    "USER_ID" AS user_id, -- User identifier
    "OPERATION_DT" AS operation_dt, -- Date and time of the operation
    "CHILD_BC_NAME" AS child_bc_name, -- Child business component name
    "NEW_VAL" AS new_val, -- New value
    "OLD_VAL" AS old_val, -- Old value
    "ITEM_IDEN_NUM" AS item_iden_num, -- Item identification number
    "AUDIT_LOG" AS audit_log, -- Audit log
    "DB_LAST_UPD" AS db_last_upd, -- Database last update timestamp
    "SYNC_DT" AS sync_dt, -- Synchronization date
    "OLD_OWNER" AS old_owner, -- Previous owner
    "NEW_OWNER" AS new_owner, -- New owner
    "OLD_SOURCE" AS old_source, -- Previous source
    "NEW_SOURCE" AS new_source, -- New source
    "NEW_STATUS" AS new_status, -- Previous status
    "OLD_STATUS" AS old_status, -- New status
    "AUDIT_SOURCE_CD" AS audit_source_cd, -- Audit source code
    "BC_BASE_TBL" AS bc_base_tbl, -- Base table for business component
    "CHILD_BC_BASE_TBL" AS child_bc_base_tbl, -- Base table for child business component
    "CHILD_RECORD_ID" AS child_record_id, -- Child record identifier
    "DB_LAST_UPD_SRC" AS db_last_upd_src, -- Database last update source
    "GROUP_NUM" AS group_num, -- Group number
    "LINK_NAME" AS link_name, -- Link name
    "NODE_NAME" AS node_name, -- Node name
    "SRC_DEST_ID" AS src_dest_id, -- Source destination identifier
    "TBL_NAME" AS tbl_name, -- Table name
    "TBL_RECORD_ID" AS tbl_record_id, -- Table record identifier
    "OLD_MODE" AS old_mode, -- Previous mode
    "NEW_MODE" AS new_mode, -- New mode
    "TRANS_LDAP" AS trans_ldap -- LDAP transaction
FROM {{ source('genai_power_bi', 'tbl_s_audit_item_temp_newprocess') }}