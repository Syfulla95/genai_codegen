-- Intermediate model for updating Old_Status and New_Status based on business rules
WITH base_data AS (
  SELECT
    a.row_id,
    a.operation_dt,
    a.old_status,
    a.new_status,
    CASE 
      WHEN a.new_status = 'Referred' THEN 'Button'
      ELSE a.new_mode
    END AS updated_status
  FROM {{ ref('stg_tblsf_casehistory_import') }} a
)
SELECT
  row_id,
  operation_dt,
  old_status,
  new_status,
  updated_status
FROM base_data