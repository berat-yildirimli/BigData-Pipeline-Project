-- build_star_schema.py::build_fact_payments() esdegeri.

with payments as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    from {{ source('bronze', 'olist_order_payments_dataset') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_purchase_timestamp
    from {{ source('bronze', 'olist_orders_dataset') }}

)

select
    pay.order_id,
    pay.payment_sequential,
    cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') as int) as date_key,
    c.customer_key,
    dpt.payment_type_key,
    pay.payment_value,
    pay.payment_installments
from payments pay
inner join orders o
    on pay.order_id = o.order_id
left join {{ ref('dim_customer') }} c
    on o.customer_id = c.customer_id
left join {{ ref('dim_payment_type') }} dpt
    on pay.payment_type = dpt.payment_type
