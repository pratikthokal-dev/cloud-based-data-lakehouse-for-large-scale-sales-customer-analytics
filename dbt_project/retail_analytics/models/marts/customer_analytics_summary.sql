{{ config(materialized='view') }}

SELECT
    customer_id,
    country,
    order_count,
    total_quantity,
    total_revenue,
    average_order_value
FROM {{ source('retail_analytics', 'customer_analytics') }}
