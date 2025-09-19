with
    source_subcategory as (
        select *
        from {{ source('erp', 'production_productsubcategory') }}
    )

    , renamed as (
        select
            cast(productsubcategoryid as int) as product_subcategory_pk
            , cast(productcategoryid as int) as product_category_fk
            , cast(name as string) as product_subcategory_name
            , cast(rowguid as string) as product_subcategory_row_guid
            , cast(modifieddate as timestamp) as product_subcategory_modified_date
        from source_subcategory
    )
    
select *
from renamed