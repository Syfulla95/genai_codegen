{{ config(materialized='view') }}

SELECT
    "ROW_ID" AS row_id, -- Unique identifier for the row
    "RECORD_ID" AS record_id, -- Record identifier
    "CREATED_BY" AS created_by, -- User who created the record
    "LAST_UPD_BY" AS last_upd_by, -- User who last updated the record
    "CREATED" AS created, -- Creation timestamp
    "LAST_UPD" AS last_upd, -- Last update timestamp
    "OPERATION_DT" AS operation_dt, -- Date and time of the operation
    "MODIFICATION_NUM" AS modification_num, -- Modification number
    "CONFLICT_ID" AS conflict_id, -- Conflict identifier
    "BUSCOMP_NAME" AS buscomp_name, -- Business component name
    "USER_ID" AS user_id, -- User identifier
    "OLD_OWNER" AS old_owner, -- Previous owner
    "NEW_OWNER" AS new_owner, -- New owner
    "OPERATION_CD" AS operation_cd, -- Operation code
    "FIELD_NAME" AS field_name -- Name of the field
FROM {{ source('genai_power_bi', 'tblsf_casehistory_import') }}