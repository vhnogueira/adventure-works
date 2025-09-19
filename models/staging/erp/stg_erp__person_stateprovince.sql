with
    source_state_province as (
        select *
        from {{ source('erp', 'person_stateprovince') }}
    )

    , renamed as (
        select
            cast(stateprovinceid as int) as state_province_pk
            , cast(stateprovincecode as string) as state_province_code
            , cast(countryregioncode as string) as country_region_fk
            , cast(isonlystateprovinceflag as boolean) as state_province_is_only_flag
            , cast(name as string) as state_province_name
            , cast(territoryid as int) as territory_fk
            , cast(rowguid as string) as state_province_row_guid
            , cast(modifieddate as timestamp) as state_province_modified_date
        from source_state_province
    )

select *
from renamed