{{ config(
    materialized='table',
    schema='staging'
) }}

-- Создаем тестовые данные для country_codes
SELECT 
  'US' as country_code,
  'United States' as country_name
UNION ALL
SELECT 
  'GB' as country_code,
  'United Kingdom' as country_name
UNION ALL
SELECT 
  'CA' as country_code,
  'Canada' as country_name
UNION ALL
SELECT 
  'AU' as country_code,
  'Australia' as country_name