
{{ config(materialized='table', schema='analytics') }}

SELECT 
    1 as customer_id,
    'Fallback Customer' as customer_name,
    'fallback@example.com' as email,
    'US' as country_code,
    1 as total_orders,
    100.0 as total_spent,
    CURRENT_TIMESTAMP as last_order_date,
    'VIP' as customer_segment,
    CURRENT_TIMESTAMP as processed_at
        