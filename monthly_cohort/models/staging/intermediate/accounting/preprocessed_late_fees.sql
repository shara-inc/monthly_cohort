{{ config(materialized='ephemeral') }}


with drawdowns_raw as (
    select * from {{ ref('drawdown_repayments_raw') }}
),

joined_data as (
        SELECT
            date_trunc(repayment_due_date, day) AS due_date,
            currency_code,
            late_fees,
            product_type
        FROM drawdowns_raw
        WHERE repayment_status NOT IN ('cancelled', 'active', 'pending')
        AND drawdowns_status NOT IN ('cancelled', 'pending')
)

select * from joined_data