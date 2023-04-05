{{ config(materialized='ephemeral') }}

with bnpl_repayments as (
    select * from {{ source('airbyte_src', 'ab_bnpl_repayments') }}
)

select id,
       currency_code,
       principal_amount,
       status,
       created_at,
       user_id,
       bnpl_drawdown_id,
       has_technical_issue_at,
       batch_no,
       late_fees,
       due_at,
       repayment_amount,
       completed_at
from bnpl_repayments