/*  
    This test ensures that the gross sales for 2011 match
    the audited accounting value from CEO Carlos Silveira:
    $12,646,112.16
*/
with
    sales_2011 as (
        select 
            sum(f.gross_total) as total
        from {{ ref('fct_sales') }} f
        inner join {{ ref('dim_dates') }} d
            on f.order_date_fk = d.date_full_date
        where d.date_year = 2011
    )

select total
from sales_2011
where total not between 12646112.14 and 12646112.18