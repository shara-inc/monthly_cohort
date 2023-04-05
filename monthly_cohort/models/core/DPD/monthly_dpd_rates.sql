{{ config(materialized='view') }}

with dpd as (
    select * from {{ ref('days_past_due') }}
),

bnpl_drawdowns as (
    select * from {{ ref('stg_bnpl_drawdowns') }}
),

bnpl_approvals as (
    select * from {{ ref('stg_bnpl_approvals') }}
),

bnpl_repayments as (
    select * from {{ ref('stg_bnpl_repayments') }}
)


SELECT
    bnpl_repayments.currency_code,
    TIMESTAMP_TRUNC(bnpl_approvals.created_at, MONTH) as onboard_month,
    TIMESTAMP_TRUNC(bnpl_repayments.due_at, MONTH) as due_month,
    SUM(bnpl_repayments.repayment_amount) AS amount_due,
    SUM(IF(bnpl_repayments.status = 'complete' and dpd.days_past_due <= 14, bnpl_repayments.repayment_amount, 0)) AS amount_repaid,
    CONCAT(ROUND(100 * SUM(IF(bnpl_repayments.status = 'complete' and dpd.days_past_due <= 0, bnpl_repayments.repayment_amount, 0)) / SUM(bnpl_repayments.repayment_amount), 2), '%') AS on_time_repayment_rate,
    CONCAT(ROUND(100 * SUM(IF(bnpl_repayments.status = 'complete' and dpd.days_past_due <= 14, bnpl_repayments.repayment_amount, 0)) / SUM(bnpl_repayments.repayment_amount), 2), '%') AS fourteen_dpd_rate,
    CONCAT(ROUND(100 * SUM(IF(bnpl_repayments.status = 'complete' and dpd.days_past_due <= 7, bnpl_repayments.repayment_amount, 0)) / SUM(bnpl_repayments.repayment_amount), 2), '%') AS seven_dpd_rate,
    CONCAT(ROUND(100 * SUM(IF(bnpl_repayments.status = 'complete', bnpl_repayments.repayment_amount, 0)) / SUM(bnpl_repayments.repayment_amount), 2), '%') AS all_time_rate
FROM bnpl_repayments
INNER JOIN dpd ON dpd.id = bnpl_repayments.id
LEFT JOIN bnpl_drawdowns
    ON bnpl_drawdowns.id = bnpl_repayments.bnpl_drawdown_id
INNER JOIN bnpl_approvals
    ON bnpl_approvals.id = bnpl_drawdowns.bnpl_approval_id
WHERE bnpl_repayments.status NOT IN ('cancelled', 'pending', 'active')
AND bnpl_drawdowns.status NOT IN ('cancelled', 'pending')
AND DATE(bnpl_repayments.due_at) <= DATE(TIMESTAMP_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
--AND DATE(bnpl_approvals.created_at) >= DATE('2021-11-01')
GROUP BY onboard_month, due_month, bnpl_repayments.currency_code
ORDER BY bnpl_repayments.currency_code, due_month, onboard_month