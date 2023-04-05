{{ config(materialized='view') }}

with drawdowns_raw as (
    select * from {{ ref('drawdown_repayments_raw') }}
),


prepayment_bonus AS (
    SELECT
        date_trunc(drawdown_completed_date, day) drawdown_date,
        currency_code,
        product_type,
        (0.15 * SUM(amount_drawn)) - SUM(overall_repayment_amount - amount_drawn) -
        SUM(amount_rebated) + SUM(total_deposit) AS `Prepayment Bonus`
    FROM drawdowns_raw
    WHERE product_type IS NOT NULL
    AND rn = 1
    GROUP BY drawdown_date, currency_code, product_type
)

SELECT * FROM prepayment_bonus
