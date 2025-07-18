{{ config(materialized='view') }}

SELECT
"ROW_ID" AS row_id, -- Unique identifier for the row
"RECORD_ID" AS record_id, -- Record identifier
"CREATED_BY" AS created_by, -- Identifier of the creator
"LAST_UPD_BY" AS last_upd_by, -- Identifier of the last updater
"CREATED" AS created, -- Timestamp when the record was created
"LAST_UPD" AS last_upd, -- Timestamp of the last update
"OPERATION_DT" AS operation_dt, -- Date and time of the operation
"MODIFICATION_NUM" AS modification_num, -- Number of modifications
"CONFLICT_ID" AS conflict_id, -- Conflict identifier
"BUSCOMP_NAME" AS buscomp_name, -- Business component name
"USER_ID" AS user_id, -- User identifier
"OLD_OWNER" AS old_owner, -- Previous owner
"NEW_OWNER" AS new_owner, -- New owner
"OPERATION_CD" AS operation_cd, -- Operation code
"FIELD_NAME" AS field_name -- Name of the field
FROM {{ source('genai_power_bi', 'TBLSF_CASEHISTORY_IMPORT') }}