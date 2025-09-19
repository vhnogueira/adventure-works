with
    source_sales_territory as (
        select *
        from {{ source('erp', 'sales_salesterritory') }}
    )

    , renamed as (
        select
            cast(territoryid as int) as sales_territory_pk
            , cast(name as string) as sales_territory_name
            , cast(countryregioncode as string) as country_region_fk
            , cast("group" as string) as sales_territory_group
            , cast(salesytd as numeric(19,4)) as sales_territory_sales_ytd
            , cast(saleslastyear as numeric(19,4)) as sales_territory_sales_last_year
            , cast(costytd as numeric(19,4)) as sales_territory_cost_ytd
            , cast(costlastyear as numeric(19,4)) as sales_territory_cost_last_year
            , cast(rowguid as string) as sales_territory_row_guid
            , cast(modifieddate as timestamp) as sales_territory_modified_date
        from source_sales_territory
    )

select *
from renamed