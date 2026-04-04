# Real-Time Streaming Data Pipeline

## Overview

This project simulates a real-time data pipeline similar to platforms like Netflix or Uber. It generates high-volume user activity events, streams them through Kafka, processes them using PySpark, and stores optimized data in AWS S3. Aggregated insights are served via PostgreSQL and orchestrated using Apache Airflow.

The system is designed to handle **500K+ events**, demonstrating scalability, fault tolerance, and efficient data processing.

---

## Tech Stack

* **Kafka** – Real-time data ingestion
* **PySpark (Structured Streaming)** – Distributed data processing
* **AWS S3 (Parquet)** – Scalable data storage
* **AWS Glue + Athena** – Schema management and querying
* **PostgreSQL** – Serving aggregated data
* **Apache Airflow** – Workflow orchestration
* **Docker Compose** – Local infrastructure setup

---

## Architecture

1. **Data Generator (Producer)** → Generates synthetic user events
2. **Kafka** → Streams events via topic `user-events`
3. **PySpark Streaming Job** → Cleans, transforms, aggregates
4. **S3 (Parquet)** → Stores partitioned data
5. **PostgreSQL** → Stores aggregated metrics
6. **Airflow** → Schedules and monitors workflows

---

## Features

* Real-time ingestion of streaming data
* Scalable distributed processing with Spark
* Efficient storage using partitioned Parquet
* Batch + streaming hybrid architecture
* Data quality checks and orchestration with Airflow
* Queryable data lake using Athena

---

## Project Workflow

### Phase 1: Data Generation

* Simulated events using Python + Faker
* Events include:

  * `user_id`
  * `event_type` (page_view, search, add_to_cart, purchase, logout)
  * `timestamp`
  * `session_id`
  * `device`
  * `geo`
* Produced to Kafka topic `user-events`

### Phase 2: Kafka Setup

* Kafka cluster deployed via Docker Compose
* Topic configured with multiple partitions
* Producer/consumer validation for data flow

### Phase 3: PySpark Streaming

* Consumes Kafka stream as DataFrame
* Key transformations:

  * JSON parsing
  * Data cleaning
  * Windowed aggregations (5-min intervals)
  * Sessionization
* Output written to S3 in Parquet format

### Phase 4: S3 + Glue + Athena

* Data stored in partitioned format:

  ```
  s3://bucket/events/year=YYYY/month=MM/event_type=TYPE/
  ```
* AWS Glue Data Catalog used for schema
* Athena enables SQL-based querying

### Phase 5: PostgreSQL (Serving Layer)

* Stores precomputed aggregates such as:

  * Daily Active Users (DAU)
  * Event counts per type
  * Hourly trends

### Phase 6: Airflow Orchestration

* DAGs for:

  * Daily batch processing
  * Data quality checks
  * S3 compaction jobs
* Ensures reliability and monitoring

---

## Performance Optimization

Achieved **~40% reduction in query latency** through:

* **Parquet format** (columnar storage)
* **Partitioning by date and event_type**

### Benchmark Strategy

1. Store raw data in CSV
2. Run Athena query (baseline)
3. Convert to partitioned Parquet
4. Re-run query and compare execution time

---

## Setup Instructions

### Prerequisites

* Docker & Docker Compose
* Python 3.9+
* AWS account (S3, Glue, Athena)

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Run Data Generator

```bash
python producer/generate_events.py
```

### 3. Start Spark Streaming Job

```bash
spark-submit streaming/job.py
```

### 4. Run Airflow

```bash
airflow standalone
```

---

## Example Use Cases

* Real-time user behavior analytics
* Monitoring engagement metrics
* Building recommendation systems
* Fraud detection pipelines

---

## Future Improvements

* Add real-time dashboard (Streamlit / React)
* Implement schema evolution handling
* Integrate Delta Lake or Iceberg
* Add alerting (Slack / PagerDuty)

---

## Author

Ynez Dias

---

## License

MIT License
