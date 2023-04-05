{{ config(materialized='ephemeral') }}

with bnpl_drawdown as (
    select * from {{ source('airbyte_src', 'ab_bnpl_drawdowns') }}
)

select id,
       created_at,
       completed_at,
       starts_at,
       repayment_period,
       amount_rebated,
       is_deleted,
       repayment_amount,
       bnpl_approval_id,
       currency_code,
       amount_drawn,
       total_deposit,
       user_id,
       status,
       JSON_EXTRACT_SCALAR(bnpl_drawdown.tags, '$[0]') as product_type
from bnpl_drawdown
where status not in ('cancelled')