-- build_star_schema.py::build_dim_product() esdegeri.

with products as (

    select
        product_id,
        product_category_name
    from {{ source('bronze', 'olist_products_dataset') }}

),

translated as (

    select
        p.product_id,
        coalesce(t.product_category_name_english, 'unknown') as product_category_name_english
    from products p
    left join {{ source('bronze', 'product_category_name_translation') }} t
        on p.product_category_name = t.product_category_name

)

select
    product_id,
    product_category_name_english,
    monotonically_increasing_id() as product_key
from translated
