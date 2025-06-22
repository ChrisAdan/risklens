select *, 1 as was_accepted from {{ ref('accepted_features') }}
where loan_repaid_binary is not null
union all
select *, 0 as was_accepted from {{ ref('rejected_features') }}