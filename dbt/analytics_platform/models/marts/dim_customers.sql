{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
  customer_id,
  customer_name,
  email,
  country_code,
  created_at,
  CASE 
    WHEN customer_id <= 2 THEN 'VIP'
    ELSE 'Standard'
  END as customer_segment,
  now() as processed_at
FROM {{ ref('stg_customers') }}