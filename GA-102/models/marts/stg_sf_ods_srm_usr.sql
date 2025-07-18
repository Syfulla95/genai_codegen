{{ config(materialized='view') }}

SELECT
"SRC_USR_ID" AS src_usr_id, -- Source user identifier
"LDAP_ID" AS ldap_id, -- LDAP identifier
"TRANS_EFF_TMSP" AS trans_eff_tmsp, -- Effective timestamp of the transaction
"TRANS_EXP_TMSP" AS trans_exp_tmsp -- Expiry timestamp of the transaction
FROM {{ source('genai_power_bi', 'SF_ODS_SRM_USR') }}