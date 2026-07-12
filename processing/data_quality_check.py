"""
Olist Big Data Pipeline - Phase 2 Data Quality Check

Her tablo icin:
  - toplam satir sayisi
  - kolon bazinda null sayisi
  - tam satir duplicate sayisi (butun kolonlar ayni)
  - dogal anahtar (natural key) bazinda duplicate sayisi

Veriyi degistirmez, sadece raporlar. HDFS'teki Parquet ciktisini okur
(processing/analysis.py'nin yazdigi ayni tablolari).

Calistirma:
  docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /app/processing/data_quality_check.py
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("olist_quality_check")

HDFS_INPUT_DIR = "hdfs://namenode:9000/data/olist"

# Her tablo icin: (tablo_adi, dogal_anahtar_kolonlari)
# Dogal anahtar = is mantigina gore o satiri "essiz" yapmasi gereken kolon(lar)
TABLES_WITH_KEYS = [
    ("olist_customers_dataset", ["customer_id"]),
    ("olist_geolocation_dataset", None),  # dogal anahtar yok, tekil zip+lat+lng kombinasyonu cok esnek
    ("olist_order_items_dataset", ["order_id", "order_item_id"]),
    ("olist_order_payments_dataset", ["order_id", "payment_sequential"]),
    ("olist_order_reviews_dataset", ["review_id"]),
    ("olist_orders_dataset", ["order_id"]),
    ("olist_products_dataset", ["product_id"]),
    ("olist_sellers_dataset", ["seller_id"]),
    ("product_category_name_translation", ["product_category_name"]),
]


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("OlistDataQualityCheck").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_table(spark: SparkSession, table: str, key_cols):
    path = f"{HDFS_INPUT_DIR}/{table}"
    logger.info("=" * 70)
    logger.info("TABLO: %s", table)

    df = spark.read.parquet(path)
    total_rows = df.count()
    logger.info("  Toplam satir: %d", total_rows)

    # --- Null sayimi ---
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    nulls_found = {k: v for k, v in null_counts.items() if v > 0}

    if nulls_found:
        logger.info("  Null iceren kolonlar:")
        for col_name, null_count in nulls_found.items():
            pct = (null_count / total_rows) * 100 if total_rows else 0
            logger.info("    - %-35s %8d null  (%.2f%%)", col_name, null_count, pct)
    else:
        logger.info("  Null deger bulunamadi.")

    # --- Tam satir duplicate ---
    distinct_rows = df.distinct().count()
    full_duplicates = total_rows - distinct_rows
    logger.info("  Tam satir duplicate sayisi: %d", full_duplicates)

    # --- Dogal anahtar duplicate ---
    if key_cols:
        key_distinct = df.select(key_cols).distinct().count()
        key_duplicates = total_rows - key_distinct
        logger.info(
            "  Dogal anahtar (%s) duplicate sayisi: %d",
            ", ".join(key_cols), key_duplicates,
        )
    else:
        logger.info("  Dogal anahtar tanimlanmadi (bu tablo icin atlandi).")

    return {
        "table": table,
        "total_rows": total_rows,
        "nulls_found": nulls_found,
        "full_duplicates": full_duplicates,
    }


def run():
    spark = create_spark_session()
    results = []

    for table, key_cols in TABLES_WITH_KEYS:
        try:
            results.append(check_table(spark, table, key_cols))
        except Exception as exc:
            logger.error("HATA: %s -> %s", table, str(exc))

    logger.info("=" * 70)
    logger.info("OZET")
    for r in results:
        flag = "TEMIZ" if not r["nulls_found"] and r["full_duplicates"] == 0 else "DIKKAT"
        logger.info(
            "  [%s] %-40s satir=%-8d null_kolon=%-3d tam_dup=%d",
            flag, r["table"], r["total_rows"], len(r["nulls_found"]), r["full_duplicates"],
        )

    spark.stop()


if __name__ == "__main__":
    run()