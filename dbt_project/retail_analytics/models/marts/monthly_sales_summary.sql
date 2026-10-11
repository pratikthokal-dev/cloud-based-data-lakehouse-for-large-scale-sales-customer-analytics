{{ config(materialized='view') }}

SELECT
    year,
    month,
    transaction_count,
    total_quantity,
    total_revenue
FROM {{ source('retail_analytics', 'monthly_sales') }}
