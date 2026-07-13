-- build_star_schema.py::build_dim_seller() esdegeri.

select
    seller_id,
    seller_city,
    seller_state,
    monotonically_increasing_id() as seller_key
from {{ source('bronze', 'olist_sellers_dataset') }}
