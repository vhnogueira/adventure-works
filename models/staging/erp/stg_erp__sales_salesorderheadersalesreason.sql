with
    source_sales_order_header_sales_reason as (
        select *
        from {{ source('erp', 'sales_salesorderheadersalesreason') }}
    )

    , renamed as (
        select
            cast(salesorderid as int) as sales_order_fk
            , cast(salesreasonid as int) as sales_reason_fk
            , cast(modifieddate as timestamp) as sales_order_sales_reason_modified_date
        from source_sales_order_header_sales_reason
    )

select *
from renamed