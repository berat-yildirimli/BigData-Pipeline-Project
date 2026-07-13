"""
olist_pipeline_dag.py

Phase 3 orkestrasyon DAG'i. Gunluk olarak sirasiyla:
  1) spark_ingest_bronze  -- Spark ile Bronze katmanini yeniden yukler
                              (processing/analysis.py, Phase 1'den beri
                              degismedi)
  2) dbt_run_silver_gold  -- dbt ile Silver + Gold katmanlarini yeniden
                              insa eder (dbt_project/models/silver,
                              models/gold)
  3) refresh_superset_datasets -- Superset dataset kayitlarini 'gold'
                              semasindaki guncel tablolarla senkronize eder

Calistirma ortami: docker-compose-airflow.yml (LocalExecutor). Task 1,
host'un /var/run/docker.sock'u scheduler container'ina mount edilmis
olmasina dayanir (bkz. Dockerfile.airflow, docker CLI kurulumu) --
mevcut spark-master container'ina "docker exec" ile bagla, Phase 1/2'de
elle calistirdigimiz komutun birebir ayni.

Operator secimleri (rapor icin):
  - Task 1 (spark_ingest_bronze): BashOperator + "docker exec" tercih
    edildi, DockerOperator degil -- amac zaten calisan spark-master
    servisine baglanmak, yeni bir ephemeral container/image
    olusturmak degil.
  - Task 2 (dbt_run_silver_gold): BashOperator -- dbt-core, Airflow
    image'inin kendi icine kurulu (bkz. Dockerfile.airflow), bu yuzden
    ekstra bir container/exec katmanina gerek yok, komut dogrudan
    scheduler process'i icinde (LocalExecutor) calisiyor.
  - Task 3 (refresh_superset_datasets): PythonOperator -- Superset REST
    API'sinin login + CSRF token + JSON payload akisi, curl zincirinden
    cok daha okunakli/guvenilir sekilde Python requests.Session ile
    yonetiliyor.
"""

import logging
import os

import pendulum
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")
HIVE_DATABASE_NAME = "Apache Hive"
GOLD_SCHEMA = "gold"
REQUEST_TIMEOUT = 30

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


# ---------------------------------------------------------------------
# Task 3: Superset dataset senkronizasyonu
# ---------------------------------------------------------------------

def _login(session: requests.Session) -> str:
    response = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": SUPERSET_USER,
            "password": SUPERSET_PASS,
            "provider": "db",
            "refresh": True,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _fetch_csrf_token(session: requests.Session) -> str:
    response = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.json()["result"]


def _build_session() -> requests.Session:
    session = requests.Session()
    token = _login(session)
    session.headers["Authorization"] = f"Bearer {token}"
    session.headers["Content-Type"] = "application/json"
    session.headers["X-CSRFToken"] = _fetch_csrf_token(session)
    return session


def _find_database_id(session: requests.Session, name: str):
    response = session.get(
        f"{SUPERSET_URL}/api/v1/database/",
        params={"page_size": 100},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    for db in response.json().get("result", []):
        if db.get("database_name") == name:
            return db["id"]
    return None


def _find_dataset_id(session: requests.Session, db_id: int, table_name: str, schema: str):
    response = session.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"page_size": 100},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    for dataset in response.json().get("result", []):
        if (
            dataset.get("table_name") == table_name
            and dataset.get("database", {}).get("id") == db_id
            and dataset.get("schema") == schema
        ):
            return dataset["id"]
    return None


def refresh_superset_datasets(**context) -> None:
    """
    register_gold_tables.py'nin Superset REST API kismiyla ayni mantik,
    ama Hive tablo olusturma kismi artik burada YOK -- onu dbt yapiyor
    (bkz. dbt_project/models/gold/). Bu fonksiyon sadece Superset
    dataset kayitlarinin 'gold' semasindaki guncel dbt tablolariyla
    uyumlu olmasini sagliyor: yoksa olusturur, varsa kolon metadata'sini
    tazeler (dbt her calistiginda tabloyu yeniden yarattigi icin sema
    degismis olabilir).
    """
    session = _build_session()

    db_id = _find_database_id(session, HIVE_DATABASE_NAME)
    if db_id is None:
        raise RuntimeError(
            f"Superset'te '{HIVE_DATABASE_NAME}' adinda bir veritabani "
            "baglantisi bulunamadi. Bu baglanti Phase 1/2'de elle "
            "olusturulmustu -- Superset UI'da kontrol et."
        )

    for table in GOLD_TABLE_NAMES:
        dataset_id = _find_dataset_id(session, db_id, table, GOLD_SCHEMA)

        if dataset_id is None:
            payload = {"database": db_id, "table_name": table, "schema": GOLD_SCHEMA}
            response = session.post(
                f"{SUPERSET_URL}/api/v1/dataset/", json=payload, timeout=REQUEST_TIMEOUT
            )
            if response.status_code in (200, 201):
                logger.info("Dataset olusturuldu: %s.%s", GOLD_SCHEMA, table)
            else:
                logger.warning(
                    "Dataset olusturulamadi: %s.%s -> %s: %s",
                    GOLD_SCHEMA, table, response.status_code, response.text,
                )
            continue

        refresh_response = session.put(
            f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}/refresh",
            timeout=REQUEST_TIMEOUT,
        )
        if refresh_response.status_code in (200, 201):
            logger.info(
                "Dataset refresh edildi: %s.%s (id=%s)", GOLD_SCHEMA, table, dataset_id
            )
        else:
            logger.warning(
                "Dataset refresh edilemedi: %s.%s (id=%s) -> %s",
                GOLD_SCHEMA, table, dataset_id, refresh_response.status_code,
            )


# ---------------------------------------------------------------------
# DAG tanimi
# ---------------------------------------------------------------------

default_args = {
    "owner": "berat",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}

with DAG(
    dag_id="daily_olist_pipeline",
    description="Bronze (Spark) -> Silver/Gold (dbt) -> Superset senkronizasyonu",
    default_args=default_args,
    schedule="0 6 * * *",  # her gun 06:00 UTC
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "phase3"],
) as dag:

    spark_ingest_bronze = BashOperator(
        task_id="spark_ingest_bronze",
        bash_command=(
            "docker exec spark-master /spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/app/processing/analysis.py"
        ),
    )

    dbt_run_silver_gold = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )

    refresh_superset = PythonOperator(
        task_id="refresh_superset_datasets",
        python_callable=refresh_superset_datasets,
    )

    spark_ingest_bronze >> dbt_run_silver_gold >> refresh_superset
