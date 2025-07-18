{{ config(materialized='table') }}

-- Intermediate model to map OLDVALUE and NEWVALUE to user names
WITH user_lookup AS (
  SELECT
    casehist.row_id,
    casehist.record_id,
    casehist.created_by,
    casehist.last_upd_by,
    casehist.created,
    casehist.last_upd,
    casehist.operation_dt,
    casehist.modification_num,
    casehist.conflict_id,
    casehist.buscomp_name,
    casehist.user_id,
    casehist.old_owner,
    casehist.new_owner,
    casehist.operation_cd,
    casehist.field_name,
    user1.name AS old_user_name,
    user2.name AS new_user_name
  FROM {{ ref('stg_tblsf_casehistory_import') }} casehist
  LEFT JOIN {{ ref('stg_tblsf_user') }} user1 ON casehist.old_owner = user1.id
  LEFT JOIN {{ ref('stg_tblsf_user') }} user2 ON casehist.new_owner = user2.id
)

SELECT *
FROM user_lookup