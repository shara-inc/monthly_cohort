{{ config(materialized='view') }}

with drawdown_volumes as (

    select * from {{ ref('drawdown_volumes') }}

),

w_rows as (
select
  *,
  row_number() over (partition by drawdown_volumes.currency_code, drawdown_volumes.onboarding_month) as rn
from drawdown_volumes

)

select
  w_rows.currency_code,
  w_rows.onboarding_month,
  w_rows.drawdown_month,
  w_rows.total_amount_drawn,
--  w_alt.total_amount_drawn -- check the first month amount
  w_rows.total_amount_drawn / w_alt.total_amount_drawn as onboarding_percentage
from w_rows
inner join w_rows as w_alt on w_rows.currency_code = w_alt.currency_code and w_rows.onboarding_month = w_alt.onboarding_month
where w_alt.rn = 1
order by w_rows.currency_code, w_rows.onboarding_month, w_rows.drawdown_month