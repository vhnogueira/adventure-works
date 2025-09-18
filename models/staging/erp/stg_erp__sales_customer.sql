with
    source_customer as (
        select *
        from {{ source('erp', 'sales_customer') }}
    )

    , renamed as (
        select
            cast(customerid as int) as customer_pk
            , cast(personid as int) as person_fk
            , cast(storeid as int) as store_fk
            , cast(territoryid as int) as territory_fk
            , cast(rowguid as string) as customer_row_guid
            , cast(modifieddate as timestamp) as customer_modified_date
            , case 
                when store_fk is not null then 'Store'
                when person_fk is not null then 'Individual'
                else 'Unknown'
            end as customer_type
        from source_customer
    )
    
select *
from renamed