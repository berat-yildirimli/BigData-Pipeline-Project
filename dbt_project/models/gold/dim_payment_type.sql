-- build_star_schema.py::build_dim_payment_type() esdegeri.

select
    payment_type,
    monotonically_increasing_id() as payment_type_key
from (
    select distinct payment_type
    from {{ source('bronze', 'olist_order_payments_dataset') }}
)
