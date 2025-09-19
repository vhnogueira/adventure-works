with
    source_sales_reason as (
        select *
        from {{ source('erp', 'sales_salesreason') }}
    )

    , renamed as (
        select
            cast(salesreasonid as int) as sales_reason_pk
            , cast(name as string) as sales_reason_name
            , cast(reasontype as string) as sales_reason_type
            , cast(modifieddate as timestamp) as sales_reason_modified_date
        from source_sales_reason
    )

select *
from renamed