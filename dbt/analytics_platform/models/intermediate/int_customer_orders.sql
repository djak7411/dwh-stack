{{
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'customers', 'orders']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        c.registered_at,
        c.signup_date,
        
        -- Order aggregates
        COUNT(o.order_id) AS total_orders,
        SUM(CASE WHEN o.order_status = 'completed' THEN o.order_amount ELSE 0 END) AS total_spent,
        MIN(o.ordered_at) AS first_order_date,
        MAX(o.ordered_at) AS last_order_date,
        
        -- Active status
        CASE 
            WHEN MAX(o.ordered_at) >= NOW() - INTERVAL 30 DAY THEN 'active'
            WHEN MAX(o.ordered_at) >= NOW() - INTERVAL 90 DAY THEN 'dormant'
            ELSE 'inactive'
        END AS activity_status
        
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY 
        c.customer_id,
        c.customer_name,
        c.email,
        c.registered_at,
        c.signup_date
)

SELECT * FROM customer_orders