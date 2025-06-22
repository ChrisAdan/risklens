select
    "Application Date" as application_date,
    "Amount Requested"::double as amount_requested,
    "Loan Title"::string as loan_title,
    "Risk_Score"::float as risk_score,
    replace("Debt-To-Income Ratio", '%', '')::float as dti_ratio,
    "Zip Code"::string as zip_code,
    "State"::string as state,
    "Employment Length"::string as employment_length,
    "Policy Code"::int as policy_code
from {{ source("raw_loans", "raw_loans_rejected" )}}