{{
  config(
    materialized='view', 
    schema='staging',
    tags=['staging', 'orders']
  )
}}

WITH source AS (
    SELECT 
        id,
        customer_id,
        amount,
        status,
        created_at
    FROM {{ source('iceberg', 'orders') }}
    WHERE _dbt_source_loaded_at >= '{{ var("start_date") }}'
),

cleaned AS (
    SELECT
        -- Primary key
        id AS order_id,
        
        -- Foreign keys
        customer_id,
        
        -- Order attributes
        CAST(amount AS DECIMAL(10, 2)) AS order_amount,
        status AS order_status,
        
        -- Timestamps
        created_at AS ordered_at,
        DATE(created_at) AS order_date,
        
        -- Metadata
        _dbt_source_loaded_at AS loaded_at
        
    FROM source
    WHERE 
        customer_id IS NOT NULL
        AND amount IS NOT NULL
        AND status IS NOT NULL
        AND created_at IS NOT NULL
)

SELECT 
    order_id,
    customer_id,
    order_amount,
    order_status,
    ordered_at,
    order_date,
    loaded_at
FROM cleaned