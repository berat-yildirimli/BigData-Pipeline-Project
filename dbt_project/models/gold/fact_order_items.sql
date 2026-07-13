-- build_star_schema.py::build_fact_order_items() esdegeri.
-- Dikkat: customer_key join'i orders.customer_id uzerinden yapiliyor
-- (order_items'in kendi customer_id kolonu yok), seller_key ve
-- product_key ise dogrudan order_items'in kendi kolonlarindan --
-- orijinal PySpark'taki join sirasiyla birebir ayni.

with order_items as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value
    from {{ source('bronze', 'olist_order_items_dataset') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_purchase_timestamp
    from {{ source('bronze', 'olist_orders_dataset') }}

)

select
    oi.order_id,
    oi.order_item_id,
    cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') as int) as date_key,
    c.customer_key,
    s.seller_key,
    p.product_key,
    oi.price,
    oi.freight_value
from order_items oi
inner join orders o
    on oi.order_id = o.order_id
left join {{ ref('dim_customer') }} c
    on o.customer_id = c.customer_id
left join {{ ref('dim_seller') }} s
    on oi.seller_id = s.seller_id
left join {{ ref('dim_product') }} p
    on oi.product_id = p.product_id
