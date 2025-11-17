{{
  config(
    materialized='table',
    schema='analytics',
    engine='MergeTree()',
    order_by='customer_id',
    tags=['marts', 'core', 'customers', 'enriched']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

country_codes AS (
    SELECT * FROM {{ ref('stg_country_codes') }}
),

customer_countries AS (
    SELECT
        c.*,
        cc.country_name,
        cc.region,
        cc.continent,
        cc.currency_code,
        cc.currency_name,
        cc.gdp_category,
        
        -- Географические сегменты
        CASE 
            WHEN cc.continent = 'North America' THEN 'NA'
            WHEN cc.continent = 'Europe' THEN 'EU'
            WHEN cc.continent = 'Asia' THEN 'AS'
            WHEN cc.continent = 'South America' THEN 'SA'
            WHEN cc.continent = 'Africa' THEN 'AF'
            WHEN cc.continent = 'Oceania' THEN 'OC'
            ELSE 'Other'
        END AS continent_group,
        
        -- Экономические сегменты
        CASE 
            WHEN cc.gdp_category = 'High Income' THEN 'Tier 1'
            WHEN cc.gdp_category = 'Upper Middle Income' THEN 'Tier 2'
            ELSE 'Tier 3'
        END AS economic_tier
        
    FROM customers c
    LEFT JOIN country_codes cc ON c.country_code = cc.country_code
)

SELECT 
    customer_id,
    customer_name,
    email,
    country_code,
    country_name,
    region,
    continent,
    continent_group,
    currency_code,
    currency_name,
    gdp_category,
    economic_tier,
    total_orders,
    total_spent,
    first_order_date,
    last_order_date,
    activity_status,
    customer_lifetime_days,
    customer_tier,
    days_between_orders,
    registered_at,
    signup_date,
    dbt_loaded_at
FROM customer_countries