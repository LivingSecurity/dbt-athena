{{ config(
    materialized='incremental',
    format='TEXTFILE',
    tags=['dbt_test_athena', 'insert_overwrite'],
    incremental_strategy='insert_overwrite',
    partitioned_by=['pk1', 'pk2'],
) }}

WITH stage_data as (
    SELECT
        CAST('key_001_{{ var("pk1") }}_{{ var("pk2") }}' as VARCHAR) as primary_key,
        CAST('{{ var("name") }}' as VARCHAR) as name,
        CAST('{{ var("email") }}' as VARCHAR) as email,
        CAST('{{ var("pk1") }}' as VARCHAR) as pk1,
        CAST('{{ var("pk2") }}' as VARCHAR) as pk2
)
SELECT
    stage_data.primary_key,
    stage_data.name,
    stage_data.email,
    stage_data.pk1,
    stage_data.pk2
FROM stage_data
