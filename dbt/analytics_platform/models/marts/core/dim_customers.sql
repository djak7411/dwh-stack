{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='customer_id',
    tags=['marts', 'core', 'dimension']
  )
}}

WITH customer_metrics AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

enriched AS (
    SELECT
        customer_id,
        customer_name,
        email,
        registered_at,
        signup_date,
        
        -- Customer metrics
        total_orders,
        total_spent,
        first_order_date,
        last_order_date,
        activity_status,
        
        -- Customer lifetime
        DATE_DIFF('day', registered_at, COALESCE(last_order_date, NOW())) AS customer_lifetime_days,
        
        -- Customer segments
        CASE 
            WHEN total_spent >= 1000 THEN 'VIP'
            WHEN total_spent >= 500 THEN 'Premium'
            WHEN total_spent >= 100 THEN 'Regular'
            WHEN total_spent > 0 THEN 'New'
            ELSE 'Prospect'
        END AS customer_tier,
        
        -- Order frequency
        CASE 
            WHEN total_orders = 0 THEN 0
            ELSE customer_lifetime_days / NULLIF(total_orders, 0)
        END AS days_between_orders,
        
        -- Metadata
        NOW() AS dbt_loaded_at
        
    FROM customer_metrics
)

SELECT 
    customer_id,
    customer_name,
    email,
    registered_at,
    signup_date,
    total_orders,
    total_spent,
    first_order_date,
    last_order_date,
    activity_status,
    customer_lifetime_days,
    customer_tier,
    days_between_orders,
    dbt_loaded_at
FROM enriched