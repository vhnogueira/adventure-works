with
    source_category as (
        select *
        from {{ source('erp', 'production_productcategory') }}
    )

    , renamed as (
        select
            cast(productcategoryid as int) as product_category_pk
            , cast(name as string) as product_category_name
            , cast(rowguid as string) as product_category_row_guid
            , cast(modifieddate as timestamp) as product_category_modified_date
        from source_category
    )
    
select *
from renamed