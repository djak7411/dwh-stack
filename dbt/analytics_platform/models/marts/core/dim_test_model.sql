
{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
    test_id,
    test_name,
    created_at
FROM {{ ref('stg_test_model') }}
