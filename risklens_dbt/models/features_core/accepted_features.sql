select
    id,
    loan_amount,
    employment_length,
    annual_income,
    null as dti_ratio,
    case when loan_amount > 0 then annual_income / loan_amount else null end as income_to_loan_ratio,
    (fico_range_low + fico_range_high)/2.0 as fico_avg,
    interest_rate/100 as int_rate_pct,
    case
        when home_ownership in ('MORTGAGE', 'OWN', 'RENT') then home_ownership
        else 'OTHER'
    end as home_ownership_cat,
    -- one-hot encoding Loan Purpose
    {%- set purpose_values = get_distinct_purposes() %}
    {%- for p in purpose_values %}
        case when purpose = '{{ p }}' then 1 else 0 end as purpose_{{ p }}{{ "," if not loop.last }}
    {%- endfor %},
    replace(term, ' months', '')::int as term_months,
    case
        when loan_status = 'Fully Paid' then 1
        when loan_status = 'Charged Off' then 0
        else null
    end as loan_repaid_binary
from {{ ref('stage_accepted') }}
