{{ config(materialized='view') }}

with drawdowns_raw as (
    select * from {{ ref('drawdown_repayments_raw') }}
),


principal as (
    SELECT
      DATE_TRUNC(repayment_created_date, day) repayment_date,
      currency_code,
      CASE
            WHEN product_type = 'shara-flex' THEN 'Flex'
            WHEN product_type = 'shara-rebate' THEN 'Rebate'
            ELSE 'Classic'
            END AS product_type,
      drawdown_repayment_period as tenor,
      SUM(principal_amount) AS principal,
      SUM(CASE
            WHEN product_type IN ('shara-rebate', 'shara-flex') THEN principal_amount
            ELSE 0
            END) * 0.15 AS rebate_interest
    FROM drawdowns_raw
    WHERE repayment_status NOT IN ('cancelled')
    GROUP BY repayment_date,currency_code, product_type, drawdown_repayment_period)

select * from principal