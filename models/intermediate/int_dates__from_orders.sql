with
    -- import models
    sales_orders as (
        select *
        from {{ ref('stg_erp__sales_salesorderheader') }}
    )
    
    -- extract all unique dates from order headers
    , all_order_dates as (
        select distinct sales_order_date as date_value
        from sales_orders
        where sales_order_date is not null
        
        union
        
        select distinct sales_order_due_date as date_value
        from sales_orders
        where sales_order_due_date is not null
        
        union
        
        select distinct sales_order_ship_date as date_value
        from sales_orders
        where sales_order_ship_date is not null
    )
    
    -- transformation: create date attributes
    , date_attributes as (
        select
            date_value as date_full_date
            , {{ dbt_utils.generate_surrogate_key(['date_value']) }} as date_pk
            , extract(year from date_value) as date_year
            , extract(quarter from date_value) as date_quarter
            , extract(month from date_value) as date_month
            , extract(day from date_value) as date_day
            -- Snowflake DAYOFWEEK: 0=Monday, 1=Tuesday...6=Sunday
            , extract(dayofweek from date_value) as date_day_of_week
            , extract(dayofyear from date_value) as date_day_of_year
            , extract(week from date_value) as date_week_of_year
            , case extract(month from date_value)
                when 1 then 'January'
                when 2 then 'February'
                when 3 then 'March'
                when 4 then 'April'
                when 5 then 'May'
                when 6 then 'June'
                when 7 then 'July'
                when 8 then 'August'
                when 9 then 'September'
                when 10 then 'October'
                when 11 then 'November'
                when 12 then 'December'
            end as date_month_name
            -- Snowflake: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
            , case extract(dayofweek from date_value)
                when 0 then 'Monday'
                when 1 then 'Tuesday'
                when 2 then 'Wednesday'
                when 3 then 'Thursday'
                when 4 then 'Friday'
                when 5 then 'Saturday'
                when 6 then 'Sunday'
            end as date_day_name
            , case extract(dayofweek from date_value)
                when 5 then true  -- Saturday
                when 6 then true  -- Sunday
                else false
            end as date_is_weekend
            -- Fiscal year starts July 1st for Adventure Works
            , case 
                when extract(month from date_value) >= 7 
                then extract(year from date_value) + 1
                else extract(year from date_value)
            end as date_fiscal_year
            , case 
                when extract(month from date_value) in (7, 8, 9) then 1
                when extract(month from date_value) in (10, 11, 12) then 2
                when extract(month from date_value) in (1, 2, 3) then 3
                when extract(month from date_value) in (4, 5, 6) then 4
            end as date_fiscal_quarter
        from all_order_dates
    )

select *
from date_attributes