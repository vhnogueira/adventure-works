/*  
    This test ensures that the gross sales for 2011 match
    the audited accounting value:
    $12,646,112.16
*/
with
    sales_2011 as (
        select sum(line_total) as total
        from {{ ref('fct_sales') }}
        where extract(year from order_date_pk) = 2011
    )

select total
from sales_2011
where total not between 12646112.14 and 12646112.18