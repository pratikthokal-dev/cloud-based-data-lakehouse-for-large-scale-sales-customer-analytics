{{ config(materialized='view') }}

SELECT
    transaction_type,
    transaction_count,
    total_quantity,
    total_revenue
FROM {{ source('retail_analytics', 'transaction_summary') }}
