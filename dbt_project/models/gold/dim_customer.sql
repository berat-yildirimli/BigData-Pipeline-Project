-- build_star_schema.py::build_dim_customer() esdegeri.
-- monotonically_increasing_id() PySpark DataFrame API'de oldugu gibi
-- Spark SQL fonksiyonu olarak da kullanilabiliyor.
--
-- NOT: monotonically_increasing_id() calisma zamaninda partition/task
-- sirasina gore uretilir, yani her `dbt run`da customer_key degerleri
-- degisebilir (stabil degildir). Bu, orijinal PySpark script'inden
-- devralinan bir sinirlama -- dbt bunu duzeltmiyor, ayni davranisi
-- koruyor. Rapor icin "Explain the challenging parts" sorusuna iyi bir
-- ornek: surrogate key stabilitesi icin dbt_utils.generate_surrogate_key
-- (hash tabanli) tercih edilebilirdi.

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    monotonically_increasing_id() as customer_key
from {{ source('bronze', 'olist_customers_dataset') }}
