with
    source_business_entity_address as (
        select *
        from {{ source('erp', 'person_businessentityaddress') }}
    )

    , renamed as (
        select
            cast(businessentityid as int) as business_entity_fk
            , cast(addressid as int) as address_fk
            , cast(addresstypeid as int) as address_type_fk
            , cast(rowguid as string) as business_entity_address_row_guid
            , cast(modifieddate as timestamp) as business_entity_address_modified_date
        from source_business_entity_address
    )

select *
from renamed