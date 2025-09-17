with
    int_customers as (
        select *
        from {{ ref('int_customers__unified') }}
    )
    
select *
from int_customers