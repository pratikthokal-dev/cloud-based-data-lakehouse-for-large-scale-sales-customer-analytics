{{ config(materialized='view') }}

SELECT
    stockcode,
    description,
    order_count,
    total_quantity,
    total_revenue,
    average_price
FROM {{ source('retail_analytics', 'product_analytics') }}
