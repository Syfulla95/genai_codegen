-- Intermediate model to process source-related transformations, including conversions of Old_Source and New_Source.

{{ config(materialized='incremental', unique_key='row_id', incremental_strategy='merge') }}

WITH source_updates AS (
  SELECT
    row_id,
    operation_dt,
    old_source,
    new_source,
    CASE 
      WHEN old_source = 'System A' THEN 'Legacy System'
      WHEN old_source = 'System B' THEN 'Modern Platform'
      ELSE old_source
    END AS updated_old_source,
    CASE 
      WHEN new_source = 'System A' THEN 'Legacy System'
      WHEN new_source = 'System B' THEN 'Modern Platform'
      ELSE new_source
    END AS updated_new_source
  FROM {{ ref('stg_tbl_s_audit_item_sf_newprocess') }}
)
SELECT
  row_id,
  operation_dt,
  updated_old_source,
  updated_new_source
FROM source_updates
{% if is_incremental() %}
WHERE operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}