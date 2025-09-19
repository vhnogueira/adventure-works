with
    products as (
        select *
        from {{ ref('stg_erp__production_product') }}
    )

    , subcategories as (
        select *
        from {{ ref('stg_erp__production_productsubcategory') }}
    )

    , categories as (
        select *
        from {{ ref('stg_erp__production_productcategory') }}
    )

    , joined as (
        select
            products.product_pk
            , products.product_name
            , products.product_number
            , products.product_make_flag
            , products.product_finished_goods_flag
            , products.product_color
            , products.product_list_price
            , products.product_size
            , products.product_weight
            , products.product_subcategory_fk
            , coalesce(subcategories.product_subcategory_name, 'No Subcategory') as product_subcategory_name
            , coalesce(categories.product_category_name, 'No Category') as product_category_name
        from products
        left join subcategories
            on products.product_subcategory_fk = subcategories.product_subcategory_pk
        left join categories
            on subcategories.product_category_fk = categories.product_category_pk
    )

select *
from joined