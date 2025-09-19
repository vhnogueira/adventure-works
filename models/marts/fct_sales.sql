{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['customer_fk'], 'type': 'btree'},
      {'columns': ['product_fk'], 'type': 'btree'},
      {'columns': ['order_date_pk'], 'type': 'btree'},
      {'columns': ['territory_fk'], 'type': 'btree'}
    ]
  )
}}

with
    int_sales as (
        select *
        from {{ ref('int_sales__order_items_enriched') }}
    )

select *
from int_sales