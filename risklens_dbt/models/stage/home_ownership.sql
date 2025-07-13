select 
    distinct home_ownership
from {{ source('raw_loans', 'raw_loans_accepted')}}
