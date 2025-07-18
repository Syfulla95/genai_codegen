-- Intermediate model to handle owner-related transformations, including LDAP stamping and group name resolution.

{{ config(materialized='incremental', unique_key='row_id', incremental_strategy='merge') }}

WITH ldap_stamping AS (
  SELECT
    a.row_id,
    a.operation_dt,
    a.old_owner,
    a.new_owner,
    u.name AS old_owner_name,
    u2.name AS new_owner_name
  FROM {{ ref('stg_tbl_s_audit_item_sf_newprocess') }} AS a
  LEFT JOIN {{ ref('stg_tblsf_user') }} AS u
    ON a.old_owner = u.id
  LEFT JOIN {{ ref('stg_tblsf_user') }} AS u2
    ON a.new_owner = u2.id
),
group_resolution AS (
  SELECT
    l.row_id,
    l.operation_dt,
    l.old_owner_name,
    l.new_owner_name,
    g.group_name AS old_owner_group,
    g2.group_name AS new_owner_group
  FROM ldap_stamping AS l
  LEFT JOIN {{ ref('stg_tblsf_group') }} AS g
    ON l.old_owner_name = g.developername
  LEFT JOIN {{ ref('stg_tblsf_group') }} AS g2
    ON l.new_owner_name = g2.developername
)
SELECT
  row_id,
  operation_dt,
  old_owner_name,
  new_owner_name,
  old_owner_group,
  new_owner_group
FROM group_resolution
{% if is_incremental() %}
WHERE operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}