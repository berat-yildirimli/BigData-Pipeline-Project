-- build_star_schema.py::build_dim_date() esdegeri.
--
-- Orijinal PySpark: min/max tarihi Python'a collect() edip f-string ile
-- spark.sql()'e gomuyordu. Burada ayni mantigi tek bir SQL sorgusunda
-- (bounds CTE'i + explode(sequence(...))) kuruyoruz -- Spark SQL'in
-- explode(sequence(...)) fonksiyonu bounds CTE'indeki tek satirin
-- min_date/max_date kolonlarina dogrudan referans verebiliyor.

with bounds as (

    select
        min(to_date(order_purchase_timestamp)) as min_date,
        max(to_date(order_purchase_timestamp)) as max_date
    from {{ source('bronze', 'olist_orders_dataset') }}

),

calendar as (

    select explode(sequence(min_date, max_date, interval 1 day)) as full_date
    from bounds

)

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    year(full_date) as year,
    month(full_date) as month,
    quarter(full_date) as quarter,
    dayofweek(full_date) as day_of_week
from calendar
