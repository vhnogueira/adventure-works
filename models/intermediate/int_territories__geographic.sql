with
    addresses as (
        select *
        from {{ ref('stg_erp__person_address') }}
    )

    , state_provinces as (
        select *
        from {{ ref('stg_erp__person_stateprovince') }}
    )

    , country_regions as (
        select *
        from {{ ref('stg_erp__person_countryregion') }}
    )

    , sales_territories as (
        select *
        from {{ ref('stg_erp__sales_salesterritory') }}
    )

    -- Join geographic information
    , geographic_enriched as (
        select
            addresses.address_pk
            , addresses.address_line_1
            , addresses.address_line_2
            , addresses.address_city
            , addresses.address_postal_code
            , state_provinces.state_province_name
            , state_provinces.state_province_code
            , country_regions.country_region_name
            , country_regions.country_region_pk as country_region_code
            , sales_territories.sales_territory_pk as territory_id
            , sales_territories.sales_territory_name
            , sales_territories.sales_territory_group
            , addresses.address_row_guid
            , addresses.address_modified_date
        from addresses
        left join state_provinces
            on addresses.state_province_fk = state_provinces.state_province_pk
        left join country_regions
            on state_provinces.country_region_fk = country_regions.country_region_pk
        left join sales_territories
            on state_provinces.territory_fk = sales_territories.sales_territory_pk
    )

    -- Create territory dimension
    , territories_final as (
        select
            address_pk as territory_pk
            , coalesce(address_city, 'Unknown') as territory_city
            , coalesce(state_province_name, 'Unknown') as territory_state_province
            , coalesce(address_postal_code, 'Unknown') as territory_postal_code
            , coalesce(country_region_name, 'Unknown') as territory_country_region
            , territory_id as territory_sales_territory_id
            , coalesce(sales_territory_name, 'No Territory') as territory_name
            , coalesce(sales_territory_group, 'No Group') as territory_group
            , address_row_guid as territory_row_guid
            , address_modified_date as territory_modified_date
        from geographic_enriched
    )

select *
from territories_final