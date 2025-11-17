{{
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'orders', 'payments']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

order_payments AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_amount,
        o.order_status,
        o.ordered_at,
        
        -- Payment aggregates
        COUNT(p.payment_id) AS payment_count,
        SUM(p.payment_amount) AS total_paid_amount,
        MAX(p.paid_at) AS last_payment_date,
        
        -- Payment status
        CASE 
            WHEN COUNT(p.payment_id) = 0 THEN 'unpaid'
            WHEN SUM(p.payment_amount) >= o.order_amount THEN 'fully_paid'
            ELSE 'partially_paid'
        END AS payment_status,
        
        -- Payment methods
        COUNT(DISTINCT p.payment_method) AS distinct_payment_methods
        
    FROM orders o
    LEFT JOIN payments p ON o.order_id = p.order_id
    GROUP BY 
        o.order_id,
        o.customer_id,
        o.order_amount,
        o.order_status,
        o.ordered_at
)

SELECT * FROM order_payments