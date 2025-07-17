{{ config(materialized='view') }}

SELECT
    "ID" AS id, -- User identifier
    "NAME" AS name -- Name of the user
FROM {{ source('genai_power_bi', 'tblsf_user') }}