{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'customers']
  )
}}

WITH source AS (
    SELECT 
        id,
        name,
        email,
        country_code,
        created_at
    FROM {{ source('iceberg', 'customers') }}
    WHERE _dbt_source_loaded_at >= '{{ var("start_date") }}'
),

cleaned AS (
    SELECT
        -- Primary key
        id AS customer_id,
        
        -- Customer attributes
        TRIM(name) AS customer_name,
        LOWER(TRIM(email)) AS email,
        UPPER(TRIM(country_code)) AS country_code,
        
        -- Timestamps
        created_at AS registered_at,
        DATE(created_at) AS signup_date,
        
        -- Metadata
        _dbt_source_loaded_at AS loaded_at
        
    FROM source
    WHERE 
        name IS NOT NULL 
        AND email IS NOT NULL
        AND created_at IS NOT NULL
)

SELECT 
    customer_id,
    customer_name,
    email,
    country_code,
    registered_at,
    signup_date,
    loaded_at
FROM cleaned