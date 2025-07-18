-- Staging model for the baddata table
WITH source_data AS (
  SELECT
    row_id,
    operation_dt,
    fieldname,
    newvalue,
    oldvalue,
    colcode,
    errorcode,
    errorcolumn
  FROM {{ source('genai_power_bi', 'baddata') }}
)
SELECT
  row_id,
  operation_dt,
  fieldname,
  newvalue,
  oldvalue,
  colcode,
  errorcode,
  errorcolumn
FROM source_data