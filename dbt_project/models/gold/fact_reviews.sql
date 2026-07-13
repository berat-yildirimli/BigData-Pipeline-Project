-- build_star_schema.py::build_fact_reviews() esdegeri.
--
-- Tasarim notu (orijinal script'ten aynen tasindi): Olist veri setinde
-- review, urun degil siparis (order_id) seviyesinde tutuluyor. Bir siparis
-- birden fazla farkli kategoriden urun icerebilir. "Kategoriye gore
-- ortalama review puani" sorusunu cevaplayabilmek icin review'i
-- order_items uzerinden urune/kategoriye bagliyoruz -- bu, bir
-- siparisteki her kalemin ayni review puanini paylasmasi anlamina gelir
-- (yaklasik bir dagilim, birebir dogru degil, ama Olist verisinde
-- review-urun arasinda dogrudan bir iliski bulunmadigi icin kabul
-- edilebilir bir yaklasim).

with order_items as (

    select
        order_id,
        product_id
    from {{ source('bronze', 'olist_order_items_dataset') }}

)

select
    r.review_id,
    r.order_id,
    cast(date_format(r.review_creation_date, 'yyyyMMdd') as int) as date_key,
    p.product_key,
    r.review_score
from {{ ref('silver_order_reviews') }} r
inner join order_items oi
    on r.order_id = oi.order_id
left join {{ ref('dim_product') }} p
    on oi.product_id = p.product_id
