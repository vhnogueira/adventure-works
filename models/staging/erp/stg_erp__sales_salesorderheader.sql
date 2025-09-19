with
    source_sales_order_header as (
        select *
        from {{ source('erp', 'sales_salesorderheader') }}
    )

    , renamed as (
        select
            cast(salesorderid as int) as sales_order_pk
            , cast(revisionnumber as int) as sales_order_revision_number
            , cast(orderdate as date) as sales_order_date
            , cast(duedate as date) as sales_order_due_date
            , cast(shipdate as date) as sales_order_ship_date
            , cast(status as int) as sales_order_status
            , cast(onlineorderflag as boolean) as sales_order_is_online
            , cast(purchaseordernumber as string) as sales_order_purchase_order_number
            , cast(accountnumber as string) as sales_order_account_number
            , cast(customerid as int) as customer_fk
            , cast(salespersonid as int) as salesperson_fk
            , cast(territoryid as int) as territory_fk
            , cast(billtoaddressid as int) as bill_to_address_fk
            , cast(shiptoaddressid as int) as ship_to_address_fk
            , cast(shipmethodid as int) as ship_method_fk
            , cast(creditcardid as int) as creditcard_fk
            , cast(creditcardapprovalcode as string) as creditcard_approval_code
            , cast(currencyrateid as int) as currency_rate_fk
            , cast(subtotal as numeric(19,4)) as sales_order_subtotal
            , cast(taxamt as numeric(19,4)) as sales_order_tax_amount
            , cast(freight as numeric(19,4)) as sales_order_freight
            , cast(totaldue as numeric(19,4)) as sales_order_total_due
            , cast(comment as string) as sales_order_comment
            , cast(rowguid as string) as sales_order_row_guid
            , cast(modifieddate as timestamp) as sales_order_modified_date
        from source_sales_order_header
    )

select *
from renamed