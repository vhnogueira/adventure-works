with
    -- Import all staging models
    sales_territories as (
        select *
        from {{ ref('stg_erp__sales_salesterritory') }}
    )

    , addresses as (
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

    -- Get all unique addresses used in orders for geography
    , order_addresses as (
        select distinct 
            oh.ship_to_address_fk as address_id
            , oh.territory_fk as sales_territory_id
        from {{ ref('stg_erp__sales_salesorderheader') }} oh
        where oh.ship_to_address_fk is not null
          and oh.territory_fk is not null
    )

    -- Build complete geographic hierarchy: Address -> StateProvince -> CountryRegion
    , address_geography as (
        select
            oa.sales_territory_id
            , a.address_pk
            , coalesce(a.address_city, 'Unknown') as territory_city
            , coalesce(sp.state_province_name, 'Unknown') as territory_state_province
            , coalesce(a.address_postal_code, 'Unknown') as territory_postal_code
            , coalesce(cr.country_region_name, 'Unknown') as territory_country_region
            , a.address_row_guid as territory_row_guid
            , a.address_modified_date as territory_modified_date
        from order_addresses oa
        inner join addresses a
            on oa.address_id = a.address_pk
        left join state_provinces sp
            on a.state_province_fk = sp.state_province_pk
        left join country_regions cr
            on sp.country_region_fk = cr.country_region_pk
    )

    -- Aggregate by sales territory to get primary geography
    , territory_primary_geography as (
        select
            sales_territory_id
            , mode(territory_city) as primary_city
            , mode(territory_state_province) as primary_state_province
            , mode(territory_country_region) as primary_country_region
            , mode(territory_postal_code) as primary_postal_code
            , min(territory_row_guid) as territory_row_guid
            , max(territory_modified_date) as territory_modified_date
        from address_geography
        group by sales_territory_id
    )

    -- Final territory dimension combining sales territory info with geography
    , territories_with_geography as (
        select
            st.sales_territory_pk as territory_pk
            , coalesce(tpg.primary_city, 'Unknown') as territory_city
            , coalesce(tpg.primary_state_province, 'Unknown') as territory_state_province
            , coalesce(tpg.primary_postal_code, 'Unknown') as territory_postal_code
            , coalesce(tpg.primary_country_region, cr.country_region_name, 'Unknown') as territory_country_region
            , st.sales_territory_pk as territory_sales_territory_id
            , coalesce(st.sales_territory_name, 'No Territory') as territory_name
            , coalesce(st.sales_territory_group, 'No Group') as territory_group
            , coalesce(tpg.territory_row_guid, st.sales_territory_row_guid) as territory_row_guid
            , coalesce(tpg.territory_modified_date, st.sales_territory_modified_date) as territory_modified_date
        from sales_territories st
        left join territory_primary_geography tpg
            on st.sales_territory_pk = tpg.sales_territory_id
        left join country_regions cr
            on st.country_region_fk = cr.country_region_pk
    )

select *
from territories_with_geography