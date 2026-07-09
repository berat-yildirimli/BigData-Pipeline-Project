"""
Olist Big Data Pipeline - Phase 1 ETL Script

CSV dosyalarini okur, Parquet formatina cevirir ve HDFS'e yazar.
"""

import logging
import traceback

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------
# Logging ayarlari
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("olist_pipeline")

# ---------------------------------------------------------------------
# Ayarlar
# ---------------------------------------------------------------------

DATA_DIR = "/app/processing/data"
HDFS_OUTPUT_DIR = "hdfs://namenode:9000/data/olist"

# Olist veri setindeki 9 CSV dosyasi
TABLES = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]


def create_spark_session() -> SparkSession:
    """Spark session olusturur."""
    spark = (
        SparkSession.builder
        .appName("OlistETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_table(spark: SparkSession, table: str) -> bool:
    """Tek bir tabloyu okur, Parquet olarak HDFS'e yazar.

    Returns
    -------
    bool
        Islem basariliysa True, hata olustuysa False.
    """
    csv_path = f"{DATA_DIR}/{table}.csv"
    parquet_path = f"{HDFS_OUTPUT_DIR}/{table}"

    try:
        logger.info("=" * 60)
        logger.info("[READ] %s", csv_path)

        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        row_count = df.count()
        logger.info("%s: %d satir okundu", table, row_count)

        logger.info("[WRITE] %s", parquet_path)
        df.write.mode("overwrite").parquet(parquet_path)

        logger.info("%s: Parquet olarak HDFS'e yazildi", table)
        return True

    except Exception as exc:
        logger.error("FAILED: %s", table)
        logger.error(str(exc))
        traceback.print_exc()
        return False


def run() -> None:
    spark = create_spark_session()

    success_count = 0
    failed_tables = []

    for table in TABLES:
        if process_table(spark, table):
            success_count += 1
        else:
            failed_tables.append(table)

    logger.info("=" * 60)
    logger.info(
        "Islem tamamlandi: %d/%d tablo basarili",
        success_count, len(TABLES),
    )
    if failed_tables:
        logger.warning("Basarisiz tablolar: %s", ", ".join(failed_tables))

    spark.stop()


if __name__ == "__main__":
    run()
