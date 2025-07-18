{{ config(materialized='view') }}

SELECT
    "ROW_ID" AS row_id, -- Unique identifier for the row
    "OPERATION_DT" AS operation_dt, -- Date and time of the operation
    "FIELDNAME" AS fieldname, -- Name of the field
    "NEWVALUE" AS newvalue, -- New value of the field
    "OLDVALUE" AS oldvalue, -- Old value of the field
    "COLCODE" AS colcode -- Column code
FROM {{ source('genai_power_bi', 'tbl_s_audit_item_parsed_log_newprocess') }}