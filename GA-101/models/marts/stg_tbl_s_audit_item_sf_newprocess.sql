{{ config(materialized='view') }}

SELECT
"ROW_ID" AS row_id, -- Unique identifier for the row
"CREATED" AS created, -- Timestamp when the record was created
"CREATED_BY" AS created_by, -- Identifier of the creator
"LAST_UPD" AS last_upd, -- Timestamp of the last update
"LAST_UPD_BY" AS last_upd_by, -- Identifier of the last updater
"MODIFICATION_NUM" AS modification_num, -- Number of modifications
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
"AUDIT_LOG" AS audit_log, -- Audit log details
"DB_LAST_UPD" AS db_last_upd, -- Database last update timestamp
"SYNC_DT" AS sync_dt, -- Synchronization date
"OLD_OWNER" AS old_owner, -- Previous owner
"NEW_OWNER" AS new_owner, -- New owner
"OLD_SOURCE" AS old_source, -- Previous source
"NEW_SOURCE" AS new_source, -- New source
"OLD_STATUS" AS old_status, -- Previous status
"NEW_STATUS" AS new_status -- New status
FROM {{ source('genai_power_bi', 'TBL_S_AUDIT_ITEM_SF_NEWPROCESS') }}