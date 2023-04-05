{{ config(materialized='ephemeral') }}

with bnpl_drawdowns as (
    select * from {{ ref('stg_bnpl_drawdowns') }}
),

bnpl_repayments as (
    select * from {{ ref('stg_bnpl_repayments') }}
),

users as (
    select * from {{ ref('stg_users') }}
)

SELECT
        bnpl_repayments.id,
        CASE
            WHEN bnpl_repayments.completed_at IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), bnpl_repayments.due_at, DAY) <= 0 THEN NULL
            ELSE GREATEST(TIMESTAMP_DIFF(COALESCE(bnpl_repayments.has_technical_issue_at, bnpl_repayments.completed_at, CURRENT_TIMESTAMP()), bnpl_repayments.due_at, DAY), 0)
        END AS days_past_due
    FROM bnpl_repayments
    LEFT JOIN bnpl_drawdowns
        ON bnpl_drawdowns.id = bnpl_repayments.bnpl_drawdown_id
    INNER JOIN users
        ON users.id = bnpl_repayments.user_id
        AND users.is_deleted = FALSE
        AND users.is_staff <> 'yes'