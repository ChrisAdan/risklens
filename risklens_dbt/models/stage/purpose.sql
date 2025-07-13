select 
distinct purpose
from {{ source('raw_loans', 'raw_loans_accepted')}}