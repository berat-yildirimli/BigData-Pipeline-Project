-- build_star_schema.py::build_silver_geolocation() esdegeri.
-- Orijinal: df.dropDuplicates() -- argumansiz, yani TUM kolonlar bazinda
-- tam satir duplicate'leri kaldirir. SQL karsiligi: SELECT DISTINCT *.
--
-- Not: Bu tablo su an hicbir gold dimension/fact tarafindan referans
-- edilmiyor -- orijinal script'te de ayni durum (temizlenip yazilir ama
-- star schema'ya dahil edilmez). Ileride geospatial analiz icin
-- kullanilabilir, bu yuzden ayri bir model olarak birakiyoruz.

select distinct *
from {{ source('bronze', 'olist_geolocation_dataset') }}
