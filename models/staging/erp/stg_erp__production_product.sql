with
    source_product as (
        select *
        from {{ source('erp', 'production_product') }}
    )

    , renamed as (
        select
            cast(productid as int) as product_pk
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(makeflag as boolean) as product_make_flag
            , cast(finishedgoodsflag as boolean) as product_finished_goods_flag
            , cast(color as string) as product_color
            , cast(safetystocklevel as int) as product_safety_stock_level
            , cast(reorderpoint as int) as product_reorder_point
            , cast(standardcost as numeric(19,4)) as product_standard_cost
            , cast(listprice as numeric(19,4)) as product_list_price
            , cast(size as string) as product_size
            , cast(sizeunitmeasurecode as string) as product_size_unit_measure_code
            , cast(weightunitmeasurecode as string) as product_weight_unit_measure_code
            , cast(weight as numeric(8,2)) as product_weight
            , cast(daystomanufacture as int) as product_days_to_manufacture
            , cast(productline as string) as product_line
            , cast(class as string) as product_class
            , cast(style as string) as product_style
            , cast(productsubcategoryid as int) as product_subcategory_fk
            , cast(productmodelid as int) as product_model_fk
            , cast(sellstartdate as timestamp) as product_sell_start_date
            , cast(sellenddate as timestamp) as product_sell_end_date
            , cast(discontinueddate as timestamp) as product_discontinued_date
            , cast(rowguid as string) as product_row_guid
            , cast(modifieddate as timestamp) as product_modified_date
        from source_product
    )
    
select *
from renamed