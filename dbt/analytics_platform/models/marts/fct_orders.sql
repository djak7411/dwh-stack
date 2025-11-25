{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
  o.order_id,
  o.customer_id,
  c.customer_name,
  o.amount,
  o.status,
  o.created_at as order_date,  -- Исправлено: created_at вместо order_date
  c.country_code,
  c.customer_segment,
  now() as processed_at
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id