-- Intermediate model for transformations of Source fields
WITH base_data AS (
  SELECT
    a.row_id,
    a.operation_dt,
    a.old_source,
    a.new_source,
    g.developername AS old_source_group,
    g2.developername AS new_source_group
  FROM {{ ref('stg_tblsf_casehistory_import') }} a
  LEFT JOIN {{ source('genai_power_bi', 'sf_ods_srm_grp') }} g
    ON a.old_source = g.src_grp_id
  LEFT JOIN {{ source('genai_power_bi', 'sf_ods_srm_grp') }} g2
    ON a.new_source = g2.src_grp_id
)
SELECT
  row_id,
  operation_dt,
  old_source,
  new_source,
  old_source_group,
  new_source_group
FROM base_data