{{ config(materialized='ephemeral') }}

with bnpl_approvals as (
    select * from {{ source('airbyte_src', 'ab_bnpl_approvals') }}
)

select created_at,
       id,
       user_id,
       is_deleted,
       status,
       interest_rate,
       monthly_revenue,
       repayment_period,
       payment_frequency
from bnpl_approvals