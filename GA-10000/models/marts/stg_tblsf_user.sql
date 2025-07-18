{{ config(materialized='view') }}

SELECT
    "ID" AS id, -- User identifier
    "NAME" AS name -- User name
FROM {{ source('genai_power_bi', 'tblsf_user') }}