with
    source_address as (
        select *
        from {{ source('erp', 'person_address') }}
    )

    , renamed as (
        select
            cast(addressid as int) as address_pk
            , cast(addressline1 as string) as address_line_1
            , cast(addressline2 as string) as address_line_2
            , cast(city as string) as address_city
            , cast(stateprovinceid as int) as state_province_fk
            , cast(postalcode as string) as address_postal_code
            , cast(spatiallocation as string) as address_spatial_location
            , cast(rowguid as string) as address_row_guid
            , cast(modifieddate as timestamp) as address_modified_date
        from source_address
    )

select *
from renamed