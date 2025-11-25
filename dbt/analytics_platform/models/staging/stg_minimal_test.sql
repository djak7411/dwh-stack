
{{ config(materialized='table', schema='analytics') }}

SELECT 
  1 as test_id,
  'airflow_test' as test_name,
  now() as created_at
