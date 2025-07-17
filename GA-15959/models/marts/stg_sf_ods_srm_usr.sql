{{ config(materialized='view') }}

SELECT
    "SRC_USR_ID" AS src_usr_id, -- Source user identifier
    "LDAP_ID" AS ldap_id, -- LDAP identifier
    "TRANS_EFF_TMSP" AS trans_eff_tmsp, -- Effective timestamp for the transaction
    "TRANS_EXP_TMSP" AS trans_exp_tmsp -- Expiration timestamp for the transaction
FROM {{ source('genai_power_bi', 'sf_ods_srm_usr') }}