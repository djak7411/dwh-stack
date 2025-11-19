
{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    1 as test_id,
    'test_value' as test_name,
    CURRENT_TIMESTAMP as created_at
