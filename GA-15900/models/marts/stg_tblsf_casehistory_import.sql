-- Staging model for the tblsf_casehistory_import table
WITH source_data AS (
  SELECT
    row_id,
    record_id,
    created_by,
    last_upd_by,
    created,
    last_upd,
    operation_dt,
    modification_num,
    conflict_id,
    buscomp_name,
    user_id,
    old_owner,
    new_owner,
    operation_cd,
    field_name
  FROM {{ source('genai_power_bi', 'tblsf_casehistory_import') }}
)
SELECT
  row_id,
  record_id,
  created_by,
  last_upd_by,
  created,
  last_upd,
  operation_dt,
  modification_num,
  conflict_id,
  buscomp_name,
  user_id,
  old_owner,
  new_owner,
  operation_cd,
  field_name
FROM source_data