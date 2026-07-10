# Big Data Analytics Pipeline — Olist E-Commerce
![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange?style=plastic) ![Hadoop HDFS](https://img.shields.io/badge/Hadoop-HDFS-yellow?style=plastic) ![Apache Hive](https://img.shields.io/badge/Apache-Hive-orange?style=plastic) ![Apache Superset](https://img.shields.io/badge/Apache-Superset-007A87?style=plastic) ![Docker](https://img.shields.io/badge/Docker-blue?style=plastic) ![Python](https://img.shields.io/badge/Python-blue?style=plastic) 

This document describes all the work completed in Phase 1: an end-to-end data pipeline (Spark → HDFS → Superset) built on the Olist Brazilian E-Commerce dataset with using **Apache Spark, Hadoop HDFS, Apache Hive,** and **Apache Superset**

---
## 💾 Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — ~100,000 real orders between 2016-2018, across 9 tables:

| Table | Row Count |
| :--- | :--- |
| olist_customers_dataset | 99,441 |
| olist_geolocation_dataset | 1,000,163 |
| olist_order_items_dataset | 112,650 |
| olist_order_payments_dataset | 103,886 |
| olist_order_reviews_dataset | 104,162 |
| olist_orders_dataset | 99,441 |
| olist_products_dataset | 32,951 |
| olist_sellers_dataset | 3,095 |
| product_category_name_translation | 71 |

CSV files are placed under `processing/data/`. This folder is excluded via `.gitignore`; the data is not committed to the repository and must be downloaded from Kaggle.




## 💻 Technologies

* Python
* Docker
* Apache Spark
* Hadoop HDFS
* Apache Hive
* Apache Superset
* SQL




## System Architecture

```text
       Olist CSV Dataset
               │
               ▼
         Apache Spark
      (CSV ➔ Parquet ETL) in analysis.py
               │
               ▼
          Hadoop HDFS
     (Distributed Storage)
               │
               ▼
         Apache Hive
       (External Tables)
               │
               ▼
        Apache Superset
   Simple Charts and Dashboard
```
## ⚙️ Data Processing Pipeline

This section details the step-by-step pipeline used for processing and analyzing the dataset.

### How the Pipeline Works

####  1. Reading the Raw Data
* The **9 Olist CSV files** are read into **Spark**.
* Spark automatically figures out each column's data type on its own (*dates, numbers, text*) instead of treating everything as plain text.

####  2. Converting to Parquet
* Each dataset is rewritten in **Parquet**, a columnar file format built for analytics.
* This keeps **file sizes smaller** and makes later queries noticeably **faster** than scanning raw CSVs.

####  3. Writing to HDFS
* The Parquet output isn't kept on a single machine's disk.
* It's written straight into **HDFS**, so the data lives on **distributed, durable storage** shared by every service in the pipeline.

####  4. Making it Queryable
* On its own, HDFS just holds files. To run **SQL** against them, external tables are registered in **Hive** (through the **Spark ThriftServer**).
* This points each table name at its HDFS folder **without copying any data**.

####  5. Visualizing the Results
* **Superset** connects to that same Hive layer and turns the tables into the charts and dashboards described above.


## 📊 Dashboard Metrics

The dashboard includes the following business insights:

* **Total Orders & Customers:** Overview of core performance metrics.
* **Temporal Analysis:** Monthly order counts and order distribution by day/hour.
* **Category Insights:** Top selling categories.
* **Geographical Distribution:** Customer distribution by state.


## 📂 Project Structure

```text
BigData-Pipeline-Project/
│
├── docker/
│
├── processing/
│   ├── analysis.py
│   └── data/
│
├── reports/
│   ├── REPORT.md
│   ├── BigData-Pipeline-Project-presentation.pdf
│   └── ScreenShots/
│       ├── dashboard 1.png
│       ├── dashboard 2.png
│       ├── Hadoop HDFS.png
│        └──Spark Master.png
│   
├── scripts/
│   ├── download_dataset.py
│   ├── setup_network.sh
│   └── setup_network.ps1
│
├── visualization/
│    └── register_tables.py
│ 
├── .gitignore
└── README.md
```
### 🎯 Project Outcomes
By the end of Phase 1, the following was working end to end:

* ✅ **Automated conversion** of all 9 Olist CSV files into Parquet using **Apache Spark**.
* ✅ **Distributed, durable storage** of the processed data in **Hadoop HDFS**.
* ✅ **SQL-queryable access** to that data through **Hive external tables** and the **Spark ThriftServer**.
* ✅ A live **Apache Superset dashboard** built directly on top of the pipeline.
* ✅ A **fully reproducible pipeline**, from raw CSV to interactive dashboard, with no manual data copying between stages.

## 📸 Screenshots

### Apache Spark

<img width="1910" height="622" alt="Spark Master" src="https://github.com/user-attachments/assets/4cbe4907-d906-4862-9a88-984719056d91" />

### Hadoop
<img width="1345" height="600" alt="Hadoop HDFS" src="https://github.com/user-attachments/assets/705a627f-fc3c-4a81-a7e6-de8c80df9946" />

### Dashboard
<img width="1892" height="746" alt="dashboard 1" src="https://github.com/user-attachments/assets/5453b01a-86e8-4a0f-a15d-67f5b6cf37b8" />
<img width="1832" height="446" alt="dashboard 2" src="https://github.com/user-attachments/assets/65b4d40a-d23b-4f6b-ab18-8af962e79235" />



## 🚀 Running the Project

Clone the repository:

```bash
git clone https://github.com/berat-yildirimli/BigData-Pipeline-Project.git
```
Create the shared network: scripts/setup_network.ps1 (Windows) or scripts/setup_network.sh

Start the required services:
```bash
docker compose -f docker/docker-compose-hdfs.yml up -d

docker compose -f docker/docker-compose-spark.yml up -d

docker compose -f docker/docker-compose-superset.yml up -d
```

Download manually the Olist dataset from Kaggle and extract the CSVs into processing/data/
or use:
```bash	
python scripts/download_dataset.py
```


Run the ETL pipeline to convert the CSVs to Parquet and write them to HDFS:
```bash
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/processing/analysis.py
```

 
Copy and run `register_tables.py` inside the Superset container (table and dataset registration):
```bash
docker cp visualization/register_tables.py superset:/tmp/register_tables.py
docker exec -it superset python /tmp/register_tables.py
```

Open:

| Service | URL |
| :---: | :---: |
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Apache Superset | http://localhost:8088 |

Default Superset credentials:

```text
Username: admin
Password: admin
```

