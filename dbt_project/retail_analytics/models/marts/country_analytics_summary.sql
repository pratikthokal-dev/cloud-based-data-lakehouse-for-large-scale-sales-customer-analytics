{{ config(materialized='view') }}

SELECT
    country,
    order_count,
    unique_customers,
    total_quantity,
    total_revenue,
    average_order_value
FROM {{ source('retail_analytics', 'country_analytics') }}
