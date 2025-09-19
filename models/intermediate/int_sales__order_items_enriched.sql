with
    -- import staging models
    order_details as (
        select *
        from {{ ref('stg_erp__sales_salesorderdetail') }}
    )

    , order_headers as (
        select *
        from {{ ref('stg_erp__sales_salesorderheader') }}
    )

    , credit_cards as (
        select *
        from {{ ref('stg_erp__sales_creditcard') }}
    )

    , special_offers as (
        select *
        from {{ ref('stg_erp__sales_specialoffer') }}
    )

    -- Get sales reasons (can be multiple per order)
    , order_sales_reasons as (
        select 
            sales_order_fk
            , listagg(sales_reason_name, ', ') as sales_reason_names
        from {{ ref('stg_erp__sales_salesorderheadersalesreason') }} osr
        inner join {{ ref('stg_erp__sales_salesreason') }} sr
            on osr.sales_reason_fk = sr.sales_reason_pk
        group by sales_order_fk
    )

    -- Main join for order items
    , order_items_base as (
        select
            -- Primary key
            {{ dbt_utils.generate_surrogate_key([
                'od.sales_order_fk', 
                'od.sales_order_detail_pk'
            ]) }} as order_item_sk

            -- Foreign keys to dimensions
            , oh.customer_fk
            , od.product_fk
            , oh.sales_order_date as order_date
            , oh.ship_to_address_fk as shipping_address_fk
            , oh.territory_fk

            -- Order attributes (denormalized)
            , oh.sales_order_pk as order_number
            , oh.sales_order_status as order_status
            , cc.credit_card_type
            , osr.sales_reason_names as sales_reason_name
            , oh.sales_order_is_online

            -- Item-level attributes
            , od.sales_order_detail_quantity as quantity
            , od.sales_order_detail_unit_price as unit_price
            , od.sales_order_detail_unit_price_discount as unit_price_discount
            , so.special_offer_description
            , so.special_offer_category

            -- Order-level amounts for allocation
            , oh.sales_order_subtotal as order_subtotal
            , oh.sales_order_tax_amount as order_tax_amount
            , oh.sales_order_freight as order_freight
            , oh.sales_order_total_due as order_total_due

            -- Dates
            , oh.sales_order_date
            , oh.sales_order_due_date
            , oh.sales_order_ship_date

        from order_details od
        inner join order_headers oh
            on od.sales_order_fk = oh.sales_order_pk
        left join credit_cards cc
            on oh.creditcard_fk = cc.credit_card_pk
        left join order_sales_reasons osr
            on oh.sales_order_pk = osr.sales_order_fk
        left join special_offers so
            on od.special_offer_fk = so.special_offer_pk
    )

    -- Calculate metrics and allocations
    , order_items_with_metrics as (
        select
            -- Primary key
            order_item_sk

            -- Foreign keys to dimensions
            , customer_fk
            , product_fk
            , cast(order_date as date) as order_date_pk  -- For joining with dim_dates
            , shipping_address_fk as territory_fk
            , territory_fk as sales_territory_fk

            -- Denormalized attributes for easier filtering
            , credit_card_type
            , sales_reason_name
            , order_status
            , order_number

            -- Measures/Metrics
            , quantity
            , unit_price
            , unit_price_discount
            , (quantity * unit_price * (1 - unit_price_discount)) as line_total

            -- Calculate allocated amounts based on line total proportion
            , case 
                when order_subtotal > 0 then
                    (quantity * unit_price * (1 - unit_price_discount)) / order_subtotal * order_freight
                else 0
            end as freight_allocated

            , case 
                when order_subtotal > 0 then
                    (quantity * unit_price * (1 - unit_price_discount)) / order_subtotal * order_tax_amount
                else 0
            end as tax_amount_allocated

            -- Additional attributes
            , sales_order_is_online
            , special_offer_description
            , special_offer_category

            -- Additional dates for analysis
            , sales_order_date
            , sales_order_due_date
            , sales_order_ship_date

            -- Calculated fields for BI
            , case 
                when unit_price_discount > 0 then true
                else false
            end as had_discount

        from order_items_base
    )

select *
from order_items_with_metrics