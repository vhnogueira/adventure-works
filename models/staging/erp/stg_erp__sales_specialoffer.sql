with
    source_special_offer as (
        select *
        from {{ source('erp', 'sales_specialoffer') }}
    )

    , renamed as (
        select
            cast(specialofferid as int) as special_offer_pk
            , cast(description as string) as special_offer_description
            , cast(discountpct as numeric(10,4)) as special_offer_discount_pct
            , cast(type as string) as special_offer_type
            , cast(category as string) as special_offer_category
            , cast(startdate as timestamp) as special_offer_start_date
            , cast(enddate as timestamp) as special_offer_end_date
            , cast(minqty as int) as special_offer_min_qty
            , cast(maxqty as int) as special_offer_max_qty
            , cast(rowguid as string) as special_offer_row_guid
            , cast(modifieddate as timestamp) as special_offer_modified_date
        from source_special_offer
    )

select *
from renamed