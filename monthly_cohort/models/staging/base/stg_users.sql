{{ config(materialized='ephemeral') }}

with users as (
    select * from {{ source('airbyte_src', 'ab_users') }}
)

select id,
       merchant_id,
       is_deleted,
       email,
       mobile,
       CONCAT(LTRIM(firstname), ' ', LTRIM(lastname)) AS fullname,
       COALESCE(JSON_EXTRACT_SCALAR(metadata, '$.staff'), 'no') as is_staff
from users


