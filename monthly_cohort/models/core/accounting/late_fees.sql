{{ config(materialized='view') }}

with preprocessed_late_fees as (
    select * from {{ ref('preprocessed_late_fees') }}
)


SELECT
    due_date,
    currency_code,
    SUM(IF(product_type IS NULL, late_fees, 0)) AS classic_late_fees,
    SUM(IF(product_type = 'shara-rebate', late_fees, 0)) AS rebate_late_fees,
    SUM(IF(product_type = 'share-flex', late_fees, 0)) AS flex_late_fees
FROM preprocessed_late_fees
GROUP BY due_date, currency_code