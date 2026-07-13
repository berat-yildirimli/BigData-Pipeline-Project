-- build_star_schema.py::build_fact_delivery() esdegeri.
-- Sadece gercekten teslim edilmis siparisler (order_delivered_customer_date dolu).

with delivered as (

    select
        order_id,
        customer_id,
        order_purchase_timestamp,
        order_delivered_customer_date
    from {{ source('bronze', 'olist_orders_dataset') }}
    where order_delivered_customer_date is not null

)

select
    d.order_id,
    cast(date_format(d.order_purchase_timestamp, 'yyyyMMdd') as int) as date_key,
    c.customer_key,
    datediff(d.order_delivered_customer_date, d.order_purchase_timestamp) as delivery_time_days
from delivered d
left join {{ ref('dim_customer') }} c
    on d.customer_id = c.customer_id
