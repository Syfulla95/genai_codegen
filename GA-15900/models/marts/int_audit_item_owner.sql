-- Intermediate model for LDAP stamping of Old_Owner and New_Owner
WITH base_data AS (
  SELECT
    a.row_id,
    a.operation_dt,
    a.old_owner,
    a.new_owner,
    u.ldap_id AS old_owner_ldap,
    u2.ldap_id AS new_owner_ldap
  FROM {{ ref('stg_tblsf_casehistory_import') }} a
  LEFT JOIN {{ source('genai_power_bi', 'sf_ods_srm_usr') }} u
    ON a.old_owner = u.src_usr_id
  LEFT JOIN {{ source('genai_power_bi', 'sf_ods_srm_usr') }} u2
    ON a.new_owner = u2.src_usr_id
)
SELECT
  row_id,
  operation_dt,
  old_owner,
  new_owner,
  old_owner_ldap,
  new_owner_ldap
FROM base_data