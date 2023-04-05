{{ config(materialized='ephemeral') }}

with bnpl_drawdowns as (
    select * from {{ ref('stg_bnpl_drawdowns') }}
),

bnpl_approvals as (
    select * from {{ ref('stg_bnpl_approvals') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

drawdown_volumes as (

         select
          date_trunc(app.created_at, month) as onboarding_month,
          date_trunc(draw.created_at, month) as drawdown_month,
          draw.currency_code,
          sum(draw.amount_drawn) as total_amount_drawn,
        from bnpl_drawdowns as draw
        inner join bnpl_approvals as app on app.id = draw.bnpl_approval_id
        inner join users on users.id = draw.user_id and users.is_deleted = false
        where is_staff != 'yes'
        and date(app.created_at) >= date('{{ var("cohort_start_date")}}')
        group by onboarding_month, drawdown_month, draw.currency_code
        order by draw.currency_code, onboarding_month, drawdown_month
)

select * from drawdown_volumes
