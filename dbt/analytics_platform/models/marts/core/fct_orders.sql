{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()', 
    order_by='order_date',
    tags=['marts', 'core', 'fact']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

order_payments AS (
    SELECT * FROM {{ ref('int_order_payments') }}
),

enriched_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_amount,
        o.order_status,
        o.ordered_at,
        o.order_date,
        
        -- Customer attributes
        c.customer_name,
        c.customer_tier,
        c.activity_status,
        
        -- Payment attributes
        op.payment_count,
        op.total_paid_amount,
        op.last_payment_date,
        op.payment_status,
        op.distinct_payment_methods,
        
        -- Financial metrics
        CASE 
            WHEN o.order_status = 'completed' THEN o.order_amount
            ELSE 0 
        END AS net_revenue,
        
        CASE 
            WHEN op.payment_status = 'fully_paid' THEN o.order_amount
            ELSE 0
        END AS paid_amount,
        
        o.order_amount - COALESCE(op.total_paid_amount, 0) AS outstanding_amount,
        
        -- Time dimensions
        toYear(o.ordered_at) AS order_year,
        toMonth(o.ordered_at) AS order_month,
        toDayOfWeek(o.ordered_at) AS order_day_of_week,
        
        -- Metadata
        o.loaded_at AS source_loaded_at,
        NOW() AS dbt_loaded_at
        
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_payments op ON o.order_id = op.order_id
)

SELECT 
    order_id,
    customer_id,
    customer_name,
    customer_tier,
    activity_status,
    order_amount,
    order_status,
    payment_count,
    total_paid_amount,
    payment_status,
    distinct_payment_methods,
    net_revenue,
    paid_amount,
    outstanding_amount,
    ordered_at,
    order_date,
    order_year,
    order_month,
    order_day_of_week,
    source_loaded_at,
    dbt_loaded_at
FROM enriched_orders