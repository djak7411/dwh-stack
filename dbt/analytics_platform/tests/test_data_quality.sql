-- Custom data quality tests

-- Test: No future dates in orders
SELECT 
    COUNT(*) AS future_orders
FROM {{ ref('fct_orders') }}
WHERE ordered_at > NOW()
HAVING COUNT(*) > 0

-- Test: Revenue consistency
SELECT 
    COUNT(*) AS revenue_mismatch
FROM {{ ref('fct_orders') }} 
WHERE order_status = 'completed' AND net_revenue != order_amount
HAVING COUNT(*) > 0

-- Test: Customer email format
SELECT 
    COUNT(*) AS invalid_emails
FROM {{ ref('dim_customers') }}
WHERE email NOT LIKE '%@%.%'
HAVING COUNT(*) > 0