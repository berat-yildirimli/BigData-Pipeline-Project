"""
Olist Big Data Pipeline - Phase 2 Star Schema Builder

Bu script iki katmani inseer eder:

  SILVER (temizlenmis):
    - olist_geolocation_dataset  -> tam satir duplicate'leri kaldirilir
    - olist_order_reviews_dataset -> review_id bazinda deduplicate edilir
      (en guncel review_answer_timestamp tutulur), order_id/review_score
      null olan satirlar cikarilir

  GOLD (star schema):
    Dimensions: dim_date, dim_customer, dim_seller, dim_product, dim_payment_type
    Facts:      fact_order_items, fact_payments, fact_delivery, fact_reviews

Sema dogrulamasi: script calismaya baslamadan once, tarih icermesi beklenen
kolonlarin gercekten TimestampType/DateType olarak okunup okunmadigini
kontrol eder (CSV'de bu kolonlar duz metin olsa da, Phase 1'de
inferSchema=True kullanildigi icin Spark bunlari otomatik olarak
timestamp'e cevirmis olmali).

Calistirma:
  docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /app/processing/build_star_schema.py
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import TimestampType, DateType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("olist_star_schema")

BRONZE_DIR = "hdfs://namenode:9000/data/olist"
SILVER_DIR = "hdfs://namenode:9000/data/silver/olist"
GOLD_DIR = "hdfs://namenode:9000/data/gold/olist"

# Sema dogrulamasinda kontrol edilecek: (tablo, [tarih_kolonlari])
DATE_COLUMNS_TO_VERIFY = {
    "olist_orders_dataset": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_order_items_dataset": ["shipping_limit_date"],
    "olist_order_reviews_dataset": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
}


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("OlistStarSchemaBuilder").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark: SparkSession, table: str):
    return spark.read.parquet(f"{BRONZE_DIR}/{table}")


# ---------------------------------------------------------------------
# Adim 0: Sema dogrulamasi
# ---------------------------------------------------------------------

def verify_date_schemas(spark: SparkSession) -> None:
    logger.info("=" * 70)
    logger.info("SEMA DOGRULAMASI: tarih kolonlari gercekten timestamp/date mi?")

    all_ok = True
    for table, date_cols in DATE_COLUMNS_TO_VERIFY.items():
        df = read_bronze(spark, table)
        schema_types = {f.name: f.dataType for f in df.schema.fields}

        for col_name in date_cols:
            dtype = schema_types.get(col_name)
            is_date_like = isinstance(dtype, (TimestampType, DateType))
            status = "OK" if is_date_like else "STRING KALMIS - DONUSUM GEREKLI"
            if not is_date_like:
                all_ok = False
            logger.info("  [%s] %s.%s -> %s", status, table, col_name, dtype)

    if all_ok:
        logger.info("Tum tarih kolonlari dogru tipte (inferSchema basarili oldu).")
    else:
        logger.warning(
            "Bazi kolonlar hala string - asagidaki adimlarda to_timestamp() ile "
            "donusturulmesi gerekecek."
        )
    logger.info("=" * 70)


# ---------------------------------------------------------------------
# SILVER katmani
# ---------------------------------------------------------------------

def build_silver_geolocation(spark: SparkSession):
    logger.info("[SILVER] olist_geolocation_dataset deduplicate ediliyor...")
    df = read_bronze(spark, "olist_geolocation_dataset")
    before = df.count()

    silver_df = df.dropDuplicates()

    after = silver_df.count()
    logger.info("  %d -> %d satir (%d duplicate kaldirildi)", before, after, before - after)

    silver_df.write.mode("overwrite").parquet(f"{SILVER_DIR}/olist_geolocation_dataset")
    return silver_df


def build_silver_reviews(spark: SparkSession):
    logger.info("[SILVER] olist_order_reviews_dataset temizleniyor...")
    df = read_bronze(spark, "olist_order_reviews_dataset")
    before = df.count()

    # order_id veya review_score null olan satirlar hicbir siparise/puana
    # baglanamiyor, cikariliyor.
    df = df.filter(F.col("order_id").isNotNull() & F.col("review_score").isNotNull())

    # Sema dogrulamasinda bu iki kolonun inferSchema tarafindan otomatik
    # timestamp'e cevrilmedigi (string kaldigi) tespit edildi. Implicit
    # cast'e guvenmek yerine acikca to_timestamp() ile donusturuyoruz.
    df = (
        df.withColumn(
            "review_creation_date",
            F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn(
            "review_answer_timestamp",
            F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"),
        )
    )

    # Ayni review_id birden fazla kez geciyorsa, en guncel
    # review_answer_timestamp'e sahip satiri tut.
    window = Window.partitionBy("review_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last())
    silver_df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    after = silver_df.count()
    logger.info("  %d -> %d satir (null/duplicate temizlendi)", before, after)

    silver_df.write.mode("overwrite").parquet(f"{SILVER_DIR}/olist_order_reviews_dataset")
    return silver_df


# ---------------------------------------------------------------------
# GOLD katmani - Dimensions
# ---------------------------------------------------------------------

def build_dim_date(spark: SparkSession, orders_df):
    logger.info("[GOLD] dim_date olusturuluyor...")

    bounds = orders_df.select(
        F.min(F.to_date("order_purchase_timestamp")).alias("min_date"),
        F.max(F.to_date("order_purchase_timestamp")).alias("max_date"),
    ).collect()[0]

    calendar_df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{bounds['min_date']}'),
            to_date('{bounds['max_date']}'),
            interval 1 day
        )) AS full_date
        """
    )

    dim_date_df = (
        calendar_df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .select("date_key", "full_date", "year", "month", "quarter", "day_of_week")
    )

    logger.info("  dim_date: %d satir", dim_date_df.count())
    dim_date_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_date")
    return dim_date_df


def build_dim_customer(spark: SparkSession):
    logger.info("[GOLD] dim_customer olusturuluyor...")
    customers_df = read_bronze(spark, "olist_customers_dataset")

    dim_customer_df = (
        customers_df
        .select("customer_id", "customer_unique_id", "customer_city", "customer_state")
        .withColumn("customer_key", F.monotonically_increasing_id())
    )

    logger.info("  dim_customer: %d satir", dim_customer_df.count())
    dim_customer_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_customer")
    return dim_customer_df


def build_dim_seller(spark: SparkSession):
    logger.info("[GOLD] dim_seller olusturuluyor...")
    sellers_df = read_bronze(spark, "olist_sellers_dataset")

    dim_seller_df = (
        sellers_df
        .select("seller_id", "seller_city", "seller_state")
        .withColumn("seller_key", F.monotonically_increasing_id())
    )

    logger.info("  dim_seller: %d satir", dim_seller_df.count())
    dim_seller_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_seller")
    return dim_seller_df


def build_dim_product(spark: SparkSession):
    logger.info("[GOLD] dim_product olusturuluyor...")
    products_df = read_bronze(spark, "olist_products_dataset")
    translation_df = read_bronze(spark, "product_category_name_translation")

    dim_product_df = (
        products_df
        .select("product_id", "product_category_name")
        .join(translation_df, on="product_category_name", how="left")
        .withColumn(
            "product_category_name_english",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
        )
        .select("product_id", "product_category_name_english")
        .withColumn("product_key", F.monotonically_increasing_id())
    )

    logger.info("  dim_product: %d satir", dim_product_df.count())
    dim_product_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_product")
    return dim_product_df


def build_dim_payment_type(spark: SparkSession):
    logger.info("[GOLD] dim_payment_type olusturuluyor...")
    payments_df = read_bronze(spark, "olist_order_payments_dataset")

    dim_payment_type_df = (
        payments_df
        .select("payment_type")
        .distinct()
        .withColumn("payment_type_key", F.monotonically_increasing_id())
    )

    logger.info("  dim_payment_type: %d satir", dim_payment_type_df.count())
    dim_payment_type_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_payment_type")
    return dim_payment_type_df


# ---------------------------------------------------------------------
# GOLD katmani - Facts
# ---------------------------------------------------------------------

def build_fact_order_items(spark: SparkSession, orders_df, dim_date_df, dim_customer_df, dim_seller_df, dim_product_df):
    logger.info("[GOLD] fact_order_items olusturuluyor...")
    order_items_df = read_bronze(spark, "olist_order_items_dataset")

    fact_df = (
        order_items_df
        .join(
            orders_df.select("order_id", "customer_id", "order_purchase_timestamp"),
            on="order_id", how="inner",
        )
        .withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .join(dim_customer_df.select("customer_id", "customer_key"), on="customer_id", how="left")
        .join(dim_seller_df.select("seller_id", "seller_key"), on="seller_id", how="left")
        .join(dim_product_df.select("product_id", "product_key"), on="product_id", how="left")
        .select(
            "order_id", "order_item_id", "date_key",
            "customer_key", "seller_key", "product_key",
            "price", "freight_value",
        )
    )

    logger.info("  fact_order_items: %d satir", fact_df.count())
    fact_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_order_items")
    return fact_df


def build_fact_payments(spark: SparkSession, orders_df, dim_customer_df, dim_payment_type_df):
    logger.info("[GOLD] fact_payments olusturuluyor...")
    payments_df = read_bronze(spark, "olist_order_payments_dataset")

    fact_df = (
        payments_df
        .join(
            orders_df.select("order_id", "customer_id", "order_purchase_timestamp"),
            on="order_id", how="inner",
        )
        .withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .join(dim_customer_df.select("customer_id", "customer_key"), on="customer_id", how="left")
        .join(dim_payment_type_df, on="payment_type", how="left")
        .select(
            "order_id", "payment_sequential", "date_key",
            "customer_key", "payment_type_key",
            "payment_value", "payment_installments",
        )
    )

    logger.info("  fact_payments: %d satir", fact_df.count())
    fact_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_payments")
    return fact_df


def build_fact_delivery(spark: SparkSession, orders_df, dim_customer_df):
    logger.info("[GOLD] fact_delivery olusturuluyor...")

    # Sadece gercekten teslim edilmis siparisler (delivered_customer_date dolu)
    delivered_df = orders_df.filter(F.col("order_delivered_customer_date").isNotNull())

    fact_df = (
        delivered_df
        .withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .withColumn(
            "delivery_time_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
        )
        .join(dim_customer_df.select("customer_id", "customer_key"), on="customer_id", how="left")
        .select("order_id", "date_key", "customer_key", "delivery_time_days")
    )

    logger.info("  fact_delivery: %d satir", fact_df.count())
    fact_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_delivery")
    return fact_df


def build_fact_reviews(spark: SparkSession, silver_reviews_df, dim_product_df):
    """
    Tasarim notu: Olist veri setinde review, urun degil siparis (order_id)
    seviyesinde tutuluyor. Bir siparis birden fazla farkli kategoriden urun
    icerebilir. "Kategoriye gore ortalama review puani" sorusunu
    cevaplayabilmek icin review'i order_items uzerinden urune/kategoriye
    baglıyoruz - bu, bir siparişteki her kalemin aynı review puanini
    paylaşması anlamına gelir (yaklaşık bir dağıtım, birebir dogru degil,
    ama Olist verisinde review-urun arasinda dogrudan bir iliski
    bulunmadigi icin kabul edilebilir bir yaklasim).
    """
    logger.info("[GOLD] fact_reviews olusturuluyor...")
    order_items_df = read_bronze(spark, "olist_order_items_dataset")

    fact_df = (
        silver_reviews_df
        .join(
            order_items_df.select("order_id", "product_id"),
            on="order_id", how="inner",
        )
        .join(dim_product_df.select("product_id", "product_key"), on="product_id", how="left")
        .withColumn("date_key", F.date_format("review_creation_date", "yyyyMMdd").cast("int"))
        .select("review_id", "order_id", "date_key", "product_key", "review_score")
    )

    logger.info("  fact_reviews: %d satir", fact_df.count())
    fact_df.write.mode("overwrite").parquet(f"{GOLD_DIR}/fact_reviews")
    return fact_df


# ---------------------------------------------------------------------
# Ana akis
# ---------------------------------------------------------------------

def run():
    spark = create_spark_session()

    verify_date_schemas(spark)

    # --- Silver ---
    build_silver_geolocation(spark)
    silver_reviews_df = build_silver_reviews(spark)

    # --- Gold: dimensions ---
    orders_df = read_bronze(spark, "olist_orders_dataset")
    dim_date_df = build_dim_date(spark, orders_df)
    dim_customer_df = build_dim_customer(spark)
    dim_seller_df = build_dim_seller(spark)
    dim_product_df = build_dim_product(spark)
    dim_payment_type_df = build_dim_payment_type(spark)

    # --- Gold: facts ---
    build_fact_order_items(spark, orders_df, dim_date_df, dim_customer_df, dim_seller_df, dim_product_df)
    build_fact_payments(spark, orders_df, dim_customer_df, dim_payment_type_df)
    build_fact_delivery(spark, orders_df, dim_customer_df)
    build_fact_reviews(spark, silver_reviews_df, dim_product_df)

    logger.info("=" * 70)
    logger.info("Star schema (Silver + Gold) basariyla olusturuldu.")

    spark.stop()


if __name__ == "__main__":
    run()