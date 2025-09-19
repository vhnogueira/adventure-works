with
    source_credit_card as (
        select *
        from {{ source('erp', 'sales_creditcard') }}
    )

    , renamed as (
        select
            cast(creditcardid as int) as credit_card_pk
            , cast(cardtype as string) as credit_card_type
            , cast(cardnumber as string) as credit_card_number
            , cast(expmonth as int) as credit_card_exp_month
            , cast(expyear as int) as credit_card_exp_year
            , cast(modifieddate as timestamp) as credit_card_modified_date
        from source_credit_card
    )

select *
from renamed