{{ config(
    materialized='incremental',
    format='ICEBERG',
    tags=['dbt_test_athena', 'iceberg'],
    partitioned_by=['partition_key'],
    incremental_strategy='merge',
    unique_key='primary_key'
) }}

WITH stage_data as (
    SELECT
        'key_001_thekey' as primary_key,
        'Tom Jones' as name,
        'tom.jones@example.com' as email,
        'thekey' as partition_key
)
SELECT
    stage_data.primary_key,
    stage_data.name,
    stage_data.email,
    stage_data.partition_key
FROM stage_data
WHERE 1=0
