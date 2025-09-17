with
    source_store as (
        select *
        from {{ source('erp', 'sales_store') }}
    )

    , renamed as (
        select
            cast(businessentityid as int) as store_pk
            , cast(name as string) as store_name
            , cast(salespersonid as int) as store_salesperson_fk
            , cast(demographics as string) as store_demographics
            , cast(rowguid as string) as store_row_guid
            , cast(modifieddate as timestamp) as store_modified_date
        from source_store
    )
    
select *
from renamed