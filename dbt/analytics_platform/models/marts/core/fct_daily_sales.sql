{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='order_date',
    tags=['marts', 'core', 'aggregated']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
    WHERE order_status = 'completed'
),

daily_aggregates AS (
    SELECT
        order_date,
        order_year,
        order_month,
        
        -- Daily metrics
        COUNT(order_id) AS daily_orders,
        COUNT(DISTINCT customer_id) AS daily_customers,
        SUM(net_revenue) AS daily_revenue,
        AVG(net_revenue) AS avg_order_value,
        
        -- Customer metrics
        SUM(CASE WHEN customer_tier = 'VIP' THEN net_revenue ELSE 0 END) AS vip_revenue,
        SUM(CASE WHEN customer_tier = 'Premium' THEN net_revenue ELSE 0 END) AS premium_revenue,
        
        -- Metadata
        NOW() AS dbt_loaded_at
        
    FROM orders
    GROUP BY 
        order_date,
        order_year,
        order_month
)

SELECT 
    order_date,
    order_year,
    order_month,
    daily_orders,
    daily_customers,
    daily_revenue,
    avg_order_value,
    vip_revenue,
    premium_revenue,
    dbt_loaded_at
FROM daily_aggregates