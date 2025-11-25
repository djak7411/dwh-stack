{{
  config(
    materialized='table',
    schema='staging'
  )
}}

SELECT 
  id as customer_id,
  name as customer_name,
  email,
  country_code,
  created_at,
  now() as processed_at
FROM {{ source('iceberg', 'customers') }}