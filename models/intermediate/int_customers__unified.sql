with
    customers as (
        select *
        from {{ ref('stg_erp__sales_customer') }}
    )

    , persons as (
        select *
        from {{ ref('stg_erp__person_person') }}
    )

    , stores as (
        select *
        from {{ ref('stg_erp__sales_store') }}
    )

    -- Clientes individuais (pessoa física)
    , individual_customers as (
        select
            c.customer_pk
            , p.person_full_name as customer_name
            , 'Individual' as customer_type
            , null as customer_account_number
            , null as customer_store_name
            , c.territory_fk
            , c.customer_row_guid
            , c.customer_modified_date
        from customers c
        inner join persons p
            on c.person_fk = p.person_pk
        where c.customer_type = 'Individual'
    )

    -- Clientes corporativos (lojas)
    , store_customers as (
        select
            c.customer_pk
            , s.store_name as customer_name
            , 'Store' as customer_type
            , null as customer_account_number
            , s.store_name as customer_store_name
            , c.territory_fk
            , c.customer_row_guid
            , c.customer_modified_date
        from customers c
        inner join stores s
            on c.store_fk = s.store_pk
        where c.customer_type = 'Store'
    )

    -- União de todos os clientes
    , unified_customers as (
        select * from individual_customers
        union all
        select * from store_customers
    )

select *
from unified_customers