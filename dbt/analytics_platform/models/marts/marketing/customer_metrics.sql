{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='customer_id',
    tags=['marts', 'marketing', 'metrics']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

customer_metrics AS (
    SELECT
        customer_id,
        customer_name,
        email,
        customer_tier,
        activity_status,
        total_orders,
        total_spent,
        first_order_date,
        last_order_date,
        customer_lifetime_days,
        days_between_orders,
        
        -- Key metrics
        CASE 
            WHEN total_orders > 0 THEN total_spent / total_orders 
            ELSE 0 
        END AS avg_order_value,
        
        -- Recency score
        CASE
            WHEN DATEDIFF(day, last_order_date, CURRENT_DATE) <= 30 THEN 'Active'
            WHEN DATEDIFF(day, last_order_date, CURRENT_DATE) <= 90 THEN 'Warm'
            ELSE 'Cold'
        END AS recency_segment,
        
        -- Frequency score
        CASE
            WHEN total_orders >= 10 THEN 'High'
            WHEN total_orders >= 5 THEN 'Medium'
            ELSE 'Low'
        END AS frequency_segment,
        
        -- Monetary score
        CASE
            WHEN total_spent >= 1000 THEN 'High'
            WHEN total_spent >= 500 THEN 'Medium'
            ELSE 'Low'
        END AS monetary_segment,
        
        NOW() AS dbt_loaded_at
        
    FROM customers
)

SELECT * FROM customer_metrics