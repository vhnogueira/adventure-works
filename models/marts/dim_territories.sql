with
    int_territories as (
        select *
        from {{ ref('int_territories__geographic') }}
    )

select *
from int_territories