with
    int_dates as (
        select *
        from {{ ref('int_dates__from_orders') }}
    )

select *
from int_dates