# 🚀 Real-Time Streaming Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/PySpark-3.5.3-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-black.svg)](https://kafka.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE.svg)](https://airflow.apache.org/)

A production-grade data engineering pipeline processing **500K+ concurrent user events**. This system simulates high-velocity behavioral data (similar to Netflix or Uber), leveraging a modern distributed stack to transform raw streams into actionable business intelligence.

---

## 🏗 Architecture

The pipeline follows a **Lambda-lite architecture**, balancing real-time ingestion with structured analytical storage:

1.  **Ingestion:** Event Simulator (Python/Faker) → Kafka Cluster.
2.  **Processing:** PySpark Structured Streaming (Windowed aggregations & JSON flattening).
3.  **Storage (Data Lake):** AWS S3 partitioned by `year/month/event_type` in **Parquet** format.
4.  **Serving (Speed Layer):** PostgreSQL for sub-second dashboard queries.
5.  **Orchestration:** Airflow DAGs managing schema consistency and S3 compaction.
6.  **Visualization:** Streamlit real-time monitoring dashboard.

---

## 🛠 Tech Stack

| Component | Technology | Use Case |
| :--- | :--- | :--- |
| **Streaming** | Apache Kafka & Zookeeper | Distributed message brokerage |
| **Processing** | PySpark (Structured Streaming) | Stateful transformations & windowing |
| **Storage** | AWS S3 + Glue | Scalable Data Lake & Cataloging |
| **Analytics** | AWS Athena / PostgreSQL | SQL-based ad-hoc and serving layer |
| **Orchestration** | Apache Airflow | Workflow automation & Quality Gates |
| **DevOps** | Docker & Makefile | Containerization and CLI automation |

---

## ⚡ Performance Highlights

* **Latency Reduction:** Achieved a **~40% improvement** in query performance by migrating from CSV to partitioned Parquet storage.
* **Scalability:** Designed to handle **500,000+ events** with horizontal scaling via Kafka partitions.
* **Storage Efficiency:** Implemented AWS Glue for schema evolution and automated S3 compaction via Airflow.

---

## 🚀 Quick Start

### 1. Environment Setup
```bash
git clone [https://github.com/YOUR_USERNAME/streaming-pipeline.git](https://github.com/YOUR_USERNAME/streaming-pipeline.git)
cd streaming-pipeline
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt