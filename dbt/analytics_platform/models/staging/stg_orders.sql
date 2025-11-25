{{
  config(
    materialized='table',
    schema='staging'
  )
}}

SELECT 
  o.id as order_id,
  o.customer_id,
  c.name as customer_name,
  o.amount,
  o.status,
  o.created_at as order_date,
  c.country_code,
  CURRENT_TIMESTAMP as processed_at
FROM {{ source('iceberg', 'orders') }} o
LEFT JOIN {{ source('iceberg', 'customers') }} c 
  ON o.customer_id = c.id