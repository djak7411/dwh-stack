{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='continent',
    tags=['marts', 'marketing', 'geography']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers_enriched') }}
),

geography_metrics AS (
    SELECT
        continent,
        continent_group,
        region,
        country_code,
        country_name,
        economic_tier,
        
        -- Customer counts
        COUNT(customer_id) AS total_customers,
        COUNT(CASE WHEN total_orders > 0 THEN customer_id END) AS active_customers,
        
        -- Financial metrics
        SUM(total_spent) AS total_revenue,
        AVG(total_spent) AS avg_customer_value,
        AVG(total_orders) AS avg_orders_per_customer,
        
        -- Customer quality
        SUM(CASE WHEN customer_tier = 'VIP' THEN total_spent ELSE 0 END) AS vip_revenue,
        COUNT(CASE WHEN customer_tier = 'VIP' THEN customer_id END) AS vip_customers
        
    FROM customers
    GROUP BY 
        continent,
        continent_group,
        region,
        country_code,
        country_name,
        economic_tier
)

SELECT 
    continent,
    continent_group,
    region,
    country_code,
    country_name,
    economic_tier,
    total_customers,
    active_customers,
    total_revenue,
    avg_customer_value,
    avg_orders_per_customer,
    vip_revenue,
    vip_customers,
    
    -- Процентные показатели
    ROUND(active_customers * 100.0 / NULLIF(total_customers, 0), 2) AS activation_rate_percent,
    ROUND(vip_customers * 100.0 / NULLIF(total_customers, 0), 2) AS vip_rate_percent,
    
    NOW() AS dbt_loaded_at
    
FROM geography_metrics