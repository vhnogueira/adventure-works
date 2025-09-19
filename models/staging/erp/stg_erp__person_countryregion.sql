with
    source_country_region as (
        select *
        from {{ source('erp', 'person_countryregion') }}
    )

    , renamed as (
        select
            cast(countryregioncode as string) as country_region_pk
            , cast(name as string) as country_region_name
            , cast(modifieddate as timestamp) as country_region_modified_date
        from source_country_region
    )

select *
from renamed