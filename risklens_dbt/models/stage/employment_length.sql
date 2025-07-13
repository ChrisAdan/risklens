with raw as (
    select
        emp_length as employment_length
    from {{ source('raw_loans', 'raw_loans_accepted')}}
    union all
    select
        "Employment Length" as employment_length
    from {{ source('raw_loans', 'raw_loans_rejected')}}
)
select 
distinct employment_length
from raw