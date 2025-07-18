{{ config(materialized='view') }}

SELECT
    "SRC_GRP_ID" AS src_grp_id, -- Source group identifier
    "DEVLPR_NM" AS devlpr_nm, -- Developer name
    "TRANS_EFF_TMSP" AS trans_eff_tmsp, -- Effective timestamp for the transaction
    "TRANS_EXP_TMSP" AS trans_exp_tmsp -- Expiry timestamp for the transaction
FROM {{ source('genai_power_bi', 'sf_ods_srm_grp') }}
```

```sql