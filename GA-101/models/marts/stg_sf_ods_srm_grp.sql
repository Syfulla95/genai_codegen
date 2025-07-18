{{ config(materialized='view') }}

SELECT
"SRC_GRP_ID" AS src_grp_id, -- Source group identifier
"DEVLPR_NM" AS devlpr_nm, -- Developer name
"TRANS_EFF_TMSP" AS trans_eff_tmsp, -- Effective timestamp of the transaction
"TRANS_EXP_TMSP" AS trans_exp_tmsp -- Expiry timestamp of the transaction
FROM {{ source('genai_power_bi', 'SF_ODS_SRM_GRP') }}