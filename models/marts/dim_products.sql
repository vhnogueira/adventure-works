with
    int_employees as (
        select *
        from {{ ref('int_products__with_categories') }}
    )

select *
from int_employees