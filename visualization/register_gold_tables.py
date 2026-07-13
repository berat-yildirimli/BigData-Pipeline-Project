"""
register_gold_tables.py

Gold katmanindaki (star schema) dim_*/fact_* tablolarini Hive Metastore'a
ve Superset'e kaydeder. register_tables.py (Bronze katmani icin) ile ayni
mantigi izler, sadece farkli bir HDFS yolunu ve farkli bir tablo listesini
hedefler.

Calistirma (superset container'inin icinden, cunku pyhive orada kurulu):

    docker cp visualization/register_gold_tables.py superset:/tmp/register_gold_tables.py
    docker exec -it superset python /tmp/register_gold_tables.py

Ortam degiskenleri (register_tables.py ile ayni varsayilanlar):
    SUPERSET_URL, SUPERSET_USER, SUPERSET_PASS, THRIFT_HOST, THRIFT_PORT
    GOLD_BASE -> varsayilan: hdfs://namenode:9000/data/gold/olist
"""

import os
import sys
import time

import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")

THRIFT_HOST = os.environ.get("THRIFT_HOST", "spark-thriftserver")
THRIFT_PORT = int(os.environ.get("THRIFT_PORT", "10000"))

GOLD_BASE = os.environ.get("GOLD_BASE", "hdfs://namenode:9000/data/gold/olist")

HIVE_DATABASE_NAME = "Apache Hive"

GOLD_TABLE_NAMES = [
    "dim_date",
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_payment_type",
    "fact_order_items",
    "fact_payments",
    "fact_delivery",
    "fact_reviews",
]


def log(message: str) -> None:
    print(f"[register_gold_tables] {message}", flush=True)


def fail(message: str) -> None:
    print(f"[register_gold_tables] HATA: {message}", file=sys.stderr, flush=True)
    sys.exit(1)


def register_hive_tables() -> None:
    try:
        from pyhive import hive
    except ImportError:
        fail(
            "pyhive bulunamadi. Bu script'i superset container'i icinden "
            "calistirdiginizdan emin olun."
        )

    log(f"ThriftServer'a baglaniliyor: {THRIFT_HOST}:{THRIFT_PORT}")
    connection = hive.connect(
        host=THRIFT_HOST, port=THRIFT_PORT, auth="NONE", database="default",
    )
    cursor = connection.cursor()

    for table in GOLD_TABLE_NAMES:
        location = f"{GOLD_BASE}/{table}"
        cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
        cursor.execute(f"CREATE TABLE `{table}` USING PARQUET LOCATION '{location}'")
        log(f"  tablo hazir: {table}")

    cursor.close()
    connection.close()
    log("Tum Gold tablolari Hive'a kaydedildi.")


def wait_until_superset_ready(timeout_seconds: int = 120) -> None:
    log(f"Superset'in ayaga kalkmasi bekleniyor: {SUPERSET_URL}")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if response.status_code == 200:
                log("Superset hazir.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    fail("Superset zaman asimi icinde ayaga kalkmadi.")


def login(session: requests.Session) -> str:
    response = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USER, "password": SUPERSET_PASS,
            "provider": "db", "refresh": True,
        },
    )
    response.raise_for_status()
    log("Superset girisi basarili.")
    return response.json()["access_token"]


def fetch_csrf_token(session: requests.Session) -> str:
    response = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    response.raise_for_status()
    return response.json()["result"]


def build_authenticated_session() -> requests.Session:
    session = requests.Session()
    token = login(session)
    session.headers["Authorization"] = f"Bearer {token}"
    session.headers["Content-Type"] = "application/json"
    session.headers["X-CSRFToken"] = fetch_csrf_token(session)
    return session


def find_database_id(session: requests.Session, name: str):
    response = session.get(f"{SUPERSET_URL}/api/v1/database/", params={"page_size": 100})
    response.raise_for_status()
    for db in response.json().get("result", []):
        if db.get("database_name") == name:
            return db["id"]
    return None


def ensure_hive_database(session: requests.Session) -> int:
    existing_id = find_database_id(session, HIVE_DATABASE_NAME)
    if existing_id is not None:
        log(f"Veritabani baglantisi zaten mevcut (id={existing_id}).")
        return existing_id

    payload = {
        "database_name": HIVE_DATABASE_NAME,
        "sqlalchemy_uri": f"hive://{THRIFT_HOST}:{THRIFT_PORT}/default",
        "expose_in_sqllab": True,
    }
    response = session.post(f"{SUPERSET_URL}/api/v1/database/", json=payload)
    response.raise_for_status()
    db_id = response.json()["id"]
    log(f"Yeni veritabani baglantisi olusturuldu (id={db_id}).")
    return db_id


def find_dataset_id(session: requests.Session, db_id: int, table_name: str):
    response = session.get(f"{SUPERSET_URL}/api/v1/dataset/", params={"page_size": 100})
    response.raise_for_status()
    for dataset in response.json().get("result", []):
        if dataset.get("table_name") == table_name and dataset.get("database", {}).get("id") == db_id:
            return dataset["id"]
    return None


def ensure_dataset(session: requests.Session, db_id: int, table_name: str) -> None:
    existing_id = find_dataset_id(session, db_id, table_name)
    if existing_id is not None:
        log(f"  dataset zaten kayitli: {table_name} (id={existing_id})")
        return

    payload = {"database": db_id, "table_name": table_name, "schema": "default"}
    response = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload)
    if response.status_code in (200, 201):
        ds_id = response.json()["id"]
        log(f"  dataset olusturuldu: {table_name} (id={ds_id})")
    else:
        log(f"  UYARI: {table_name} kaydedilemedi -> {response.status_code}: {response.text}")


def main() -> None:
    wait_until_superset_ready()
    register_hive_tables()

    session = build_authenticated_session()
    db_id = ensure_hive_database(session)

    log(f"{len(GOLD_TABLE_NAMES)} Gold tablosu icin dataset kaydi baslatiliyor...")
    for table in GOLD_TABLE_NAMES:
        ensure_dataset(session, db_id, table)

    log("Tamamlandi.")


if __name__ == "__main__":
    main()