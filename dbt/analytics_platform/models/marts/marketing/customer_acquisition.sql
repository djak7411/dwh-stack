{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='signup_date',
    tags=['marts', 'marketing', 'acquisition']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

acquisition_metrics AS (
    SELECT
        signup_date,
        toYear(signup_date) AS signup_year,
        toMonth(signup_date) AS signup_month,
        
        -- Acquisition metrics
        COUNT(customer_id) AS new_customers,
        SUM(total_orders) AS total_orders_from_cohort,
        SUM(total_spent) AS total_revenue_from_cohort,
        
        -- Activation rate (customers who made at least one purchase)
        COUNT(CASE WHEN total_orders > 0 THEN customer_id END) AS activated_customers,
        
        -- Average metrics
        AVG(total_spent) AS avg_lifetime_value,
        AVG(total_orders) AS avg_orders_per_customer
        
    FROM customers
    GROUP BY 
        signup_date,
        signup_year,
        signup_month
)

SELECT 
    signup_date,
    signup_year,
    signup_month,
    new_customers,
    activated_customers,
    total_orders_from_cohort,
    total_revenue_from_cohort,
    avg_lifetime_value,
    avg_orders_per_customer,
    
    -- Activation rate
    ROUND(activated_customers * 100.0 / NULLIF(new_customers, 0), 2) AS activation_rate_percent,
    
    NOW() AS dbt_loaded_at
    
FROM acquisition_metrics