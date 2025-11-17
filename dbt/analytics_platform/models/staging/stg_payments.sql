{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'payments']
  )
}}

-- Предполагаем, что есть таблица payments в Iceberg
WITH source AS (
    SELECT 
        id,
        order_id,
        amount,
        payment_method,
        status,
        created_at
    FROM {{ source('iceberg', 'payments') }}
    WHERE _dbt_source_loaded_at >= '{{ var("start_date") }}'
),

cleaned AS (
    SELECT
        id AS payment_id,
        order_id,
        CAST(amount AS DECIMAL(10, 2)) AS payment_amount,
        payment_method,
        status AS payment_status,
        created_at AS paid_at,
        _dbt_source_loaded_at AS loaded_at
    FROM source
)

SELECT * FROM cleaned