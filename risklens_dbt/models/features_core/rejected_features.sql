with median_incomes as (
    select
        employment_length,
        median(annual_income) as med_annual_income
    from {{ ref('stage_accepted') }}
    where annual_income is not null
    group by 1
)

select
    null as id,
    loans.amount_requested::double as loan_amount,
    loans.employment_length::string as employment_length,
    meds.med_annual_income as annual_income,
    loans.dti_ratio,
    null as income_to_loan_ratio,
    null as fico_avg,
    null as int_rate_pct,
    null as home_ownership_cat,
    -- one-hot encoding Loan Purpose
    {%- set purpose_values = get_distinct_purposes() %}
    {%- for p in purpose_values %}
        null as purpose_{{ p }}{{ "," if not loop.last }}
    {%- endfor %},
    null as term_months,
    null as loan_repaid_binary
from {{ ref('stage_rejected') }} as loans
left join median_incomes meds
    using(employment_length)