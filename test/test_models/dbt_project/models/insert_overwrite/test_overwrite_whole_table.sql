{{ config(
    materialized='incremental',
    format='TEXTFILE',
    tags=['dbt_test_athena', 'insert_overwrite'],
    incremental_strategy='insert_overwrite',
) }}

WITH stage_data as (
    SELECT
        CAST('key_001' as VARCHAR) as primary_key,
        CAST('{{ var("name") }}' as VARCHAR) as name,
        CAST('{{ var("email") }}' as VARCHAR) as email
)
SELECT
    stage_data.primary_key,
    stage_data.name,
    stage_data.email
FROM stage_data
