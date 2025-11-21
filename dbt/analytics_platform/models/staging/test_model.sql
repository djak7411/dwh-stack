
{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
  1 as customer_id,
  'Test Customer' as customer_name,
  'test@example.com' as email,
  'US' as country_code,
  now() as created_at,
  now() as processed_at
