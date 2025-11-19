
-- This is a simple test model that should always work
{{ config(materialized='view') }}

SELECT 
    1 as id,
    'test' as name,
    CURRENT_TIMESTAMP as created_at
