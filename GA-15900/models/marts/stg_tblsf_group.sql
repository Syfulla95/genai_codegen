{{ config(materialized='view') }}

SELECT
    "ID" AS id, -- Group identifier
    "DEVELOPERNAME" AS developername, -- Developer name
    "GROUP_NAME" AS group_name -- Name of the group
FROM {{ source('genai_power_bi', 'tblsf_group') }}