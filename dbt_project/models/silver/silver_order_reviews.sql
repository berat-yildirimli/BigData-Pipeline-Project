-- build_star_schema.py::build_silver_reviews() esdegeri.
--
-- 1) order_id veya review_score null olan satirlar cikarilir (hicbir
--    siparise/puana baglanamiyorlar).
-- 2) review_creation_date / review_answer_timestamp inferSchema tarafindan
--    otomatik timestamp'e cevrilmedigi (string kaldigi) icin acikca
--    to_timestamp() ile donusturuluyor -- ayni format string: 'yyyy-MM-dd HH:mm:ss'.
-- 3) Ayni review_id birden fazla kez geciyorsa, en guncel
--    review_answer_timestamp'e sahip satir tutuluyor (PySpark'taki
--    Window.partitionBy("review_id").orderBy(desc_nulls_last) esdegeri).

with filtered as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss') as review_creation_date,
        to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss') as review_answer_timestamp
    from {{ source('bronze', 'olist_order_reviews_dataset') }}
    where order_id is not null
      and review_score is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by review_id
            order by review_answer_timestamp desc nulls last
        ) as rn
    from filtered

)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
from deduped
where rn = 1
