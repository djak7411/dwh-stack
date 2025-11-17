{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='payment_date',
    tags=['marts', 'core', 'payments']
  )
}}

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

enriched_payments AS (
    SELECT
        p.payment_id,
        p.order_id,
        p.payment_amount,
        p.payment_method,
        p.payment_status,
        p.paid_at,
        p.payment_date,
        
        -- Order context
        o.customer_id,
        o.customer_name,
        o.customer_tier,
        o.order_amount,
        o.order_status,
        
        -- Payment timing
        CASE 
            WHEN p.payment_status = 'completed' THEN 1
            ELSE 0
        END AS is_successful_payment,
        
        -- Metadata
        p.loaded_at AS source_loaded_at,
        NOW() AS dbt_loaded_at
        
    FROM payments p
    LEFT JOIN orders o ON p.order_id = o.order_id
)

SELECT 
    payment_id,
    order_id,
    customer_id,
    customer_name,
    customer_tier,
    payment_amount,
    payment_method,
    payment_status,
    order_amount,
    order_status,
    paid_at,
    payment_date,
    is_successful_payment,
    source_loaded_at,
    dbt_loaded_at
FROM enriched_payments