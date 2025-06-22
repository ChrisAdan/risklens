select
    id::string as id,
    loan_amnt as loan_amount,
    int_rate as interest_rate,
    annual_inc as annual_income,
    emp_length as employment_length,
    dti::float as dti_ratio,
    term,
    grade,
    home_ownership,
    addr_state,
    verification_status,
    purpose,
    fico_range_low,
    fico_range_high,
    loan_status
from {{ source('raw_loans', 'raw_loans_accepted') }}
where loan_status is not null