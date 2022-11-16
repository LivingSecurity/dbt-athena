{{ config(
    materialized='incremental',
    format='TEXTFILE',
    tags=['dbt_test_athena', 'insert_overwrite'],
    incremental_strategy='insert_overwrite',
    partitioned_by=['partition_key'],
) }}

WITH stage_data as (
    SELECT
        CAST('key_001_{{ var("partition_key") }}' as VARCHAR) as primary_key,
        CAST('{{ var("name") }}' as VARCHAR) as name,
        CAST('{{ var("email") }}' as VARCHAR) as email,
        CAST('{{ var("partition_key") }}' as VARCHAR) as partition_key
)
SELECT
    stage_data.primary_key,
    stage_data.name,
    stage_data.email,
    stage_data.partition_key
FROM stage_data
