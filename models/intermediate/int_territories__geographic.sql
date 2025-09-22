with
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

    , territories_with_geography as (
        select
            st.sales_territory_pk as territory_pk
            , st.sales_territory_name as territory_name
            , st.sales_territory_group as territory_group
            , cr.country_region_name as territory_country_region
            , st.sales_territory_pk as territory_sales_territory_id
            , 'Sales Territory' as territory_city
            , cr.country_region_name as territory_state_province
            , 'N/A' as territory_postal_code
            , st.sales_territory_row_guid as territory_row_guid
            , st.sales_territory_modified_date as territory_modified_date
        from sales_territories st
        left join country_regions cr
            on st.country_region_fk = cr.country_region_pk
    )

select *
from territories_with_geography