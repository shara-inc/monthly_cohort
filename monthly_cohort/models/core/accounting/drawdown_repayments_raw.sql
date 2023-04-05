{{ config(materialized='view') }}

with bnpl_drawdowns as (
    select * from {{ ref('stg_bnpl_drawdowns') }}
),

bnpl_repayments as (
    select * from {{ ref('stg_bnpl_repayments') }}
),

bnpl_approvals as (
    select * from {{ ref('stg_bnpl_approvals') }}
),

users as (
    select * from {{ ref('stg_users') }}
)


SELECT
        DATE(bnpl_repayments.completed_at) AS repayment_date,
        DATE(bnpl_repayments.due_at) AS repayment_due_date,
        bnpl_drawdowns.id AS drawdown_id,
        bnpl_drawdowns.user_id AS user_id,
        bnpl_drawdowns.currency_code AS currency_code,
        bnpl_repayments.id as repayment_id,
        DATE(bnpl_repayments.created_at) as repayment_created_date,
        bnpl_repayments.batch_no,
        bnpl_repayments.principal_amount,
        bnpl_repayments.repayment_amount,
        bnpl_repayments.late_fees,
        bnpl_repayments.status AS repayment_status,
        bnpl_drawdowns.amount_drawn,
        bnpl_drawdowns.amount_rebated,
        bnpl_drawdowns.repayment_period AS drawdown_repayment_period,
        (bnpl_repayments.repayment_amount - bnpl_repayments.principal_amount) AS interest_accrued,
        bnpl_drawdowns.total_deposit,
        bnpl_drawdowns.repayment_amount as overall_repayment_amount,
        bnpl_drawdowns.is_deleted AS drawdown_deleted,
        bnpl_drawdowns.status AS drawdowns_status,
        bnpl_drawdowns.product_type,
        bnpl_approvals.status as approval_status,
        bnpl_approvals.created_at as approval_date,
        bnpl_approvals.interest_rate,
        bnpl_approvals.monthly_revenue,
        bnpl_approvals.repayment_period,
        bnpl_approvals.payment_frequency,
        DATE(bnpl_drawdowns.created_at) AS drawdown_date,
        DATE(bnpl_drawdowns.starts_at) AS drawdown_starts_at,
        DATE(bnpl_drawdowns.completed_at) AS drawdown_completed_date,
        users.merchant_id,
        users.email,
        users.mobile,
        users.fullname,
        row_number() OVER(PARTITION BY bnpl_drawdowns.id) as rn
    FROM bnpl_drawdowns
    LEFT JOIN bnpl_repayments
        ON bnpl_drawdowns.id = bnpl_repayments.bnpl_drawdown_id
    LEFT JOIN bnpl_approvals
        ON bnpl_approvals.id = bnpl_drawdowns.bnpl_approval_id
    LEFT JOIN users
        ON users.id = bnpl_drawdowns.user_id
    WHERE users.is_staff != 'yes'
    AND users.is_deleted = FALSE