with
    source_sales_order_detail as (
        select *
        from {{ source('erp', 'sales_salesorderdetail') }}
    )

    , renamed as (
        select
            cast(salesorderid as int) as sales_order_fk
            , cast(salesorderdetailid as int) as sales_order_detail_pk
            , cast(carriertrackingnumber as string) as sales_order_detail_tracking_number
            , cast(orderqty as int) as sales_order_detail_quantity
            , cast(productid as int) as product_fk
            , cast(specialofferid as int) as special_offer_fk
            , cast(unitprice as numeric(19,4)) as sales_order_detail_unit_price
            , cast(unitpricediscount as numeric(19,4)) as sales_order_detail_unit_price_discount
            , cast(rowguid as string) as sales_order_detail_row_guid
            , cast(modifieddate as timestamp) as sales_order_detail_modified_date
        from source_sales_order_detail
    )

select *
from renamed