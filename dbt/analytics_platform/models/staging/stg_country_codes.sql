{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging', 'reference', 'countries']
  )
}}

SELECT 
    country_code,
    country_name,
    region,
    continent,
    iso_alpha3,
    currency_code,
    currency_name,
    phone_code,
    gdp_category
FROM {{ ref('country_codes') }}