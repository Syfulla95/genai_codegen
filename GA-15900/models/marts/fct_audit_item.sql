-- Final model consolidating all audit item transformations
WITH owner_data AS (
  SELECT
    row_id,
    operation_dt,
    old_owner_ldap,
    new_owner_ldap
  FROM {{ ref('int_audit_item_owner') }}
),
status_data AS (
  SELECT
    row_id,
    operation_dt,
    updated_status
  FROM {{ ref('int_audit_item_status') }}
),
source_data AS (
  SELECT
    row_id,
    operation_dt,
    old_source_group,
    new_source_group
  FROM {{ ref('int_audit_item_source') }}
)
SELECT
  o.row_id,
  o.operation_dt,
  o.old_owner_ldap,
  o.new_owner_ldap,
  s.updated_status,
  src.old_source_group,
  src.new_source_group
FROM owner_data o
LEFT JOIN status_data s
  ON o.row_id = s.row_id
LEFT JOIN source_data src
  ON o.row_id = src.row_id
{% if is_incremental() %}
WHERE o.operation_dt > (SELECT MAX(operation_dt) FROM {{ this }})
{% endif %}