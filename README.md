# Real-Time Streaming Data Pipeline

## Overview
This project simulates a real-time data pipeline similar to platforms like Netflix or Uber. It streams **500K+ synthetic user events** through Kafka, processes them with PySpark Structured Streaming, writes the output to S3 (or local Parquet), serves the aggregated data via PostgreSQL, runs orchestrations via Airflow, and visualizes live metrics in a custom Streamlit dashboard (**StreamPulse**).

<img width="1536" height="1024" alt="Real Time Data Pipeline Flow" src="https://github.com/user-attachments/assets/9bdaa254-bf95-4ea3-9563-503b4ad8a070" />


## Tech Stack
* **Kafka**: Real-time event ingestion (`kafka/`)
* **PySpark (Structured Streaming)**: Distributed data processing (`spark/`)
* **AWS S3 / Local Parquet**: Scalable partitioned storage (`output/`)
* **PostgreSQL**: Serving precomputed data aggregates (`storage/`)
* **Apache Airflow**: Workflow orchestration and data quality checks (`airflow/`)
* **Streamlit / Plotly**: Live presentation layer (`dashboard/`)

## Architecture
1. **Producer** (`kafka/producer.py`) → generates high-volume simulated events using Faker.
2. **Kafka** (`kafka/docker-compose.yml`) → handles real-time streams via topic `user-events`.
3. **PySpark Job** (`spark/streaming_job.py`) → consumes streams, parses JSON payloads, aggregates on tumbling 5-minute windows, and writes to Parquet.
4. **PostgreSQL** (`storage/postgres_writer.py`) → ingests windowed aggregates into table `event_aggregates`.
5. **Airflow** (`airflow/dags/`) → orchestrates daily pipeline runs and runs Python-based data quality checks.
6. **Dashboard** (`dashboard/app.py`) → visualizes everything dynamically.

## Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Java 11 (for PySpark)

## Setup instructions
1. Ensure your `.env` contains valid configurations (copy `.env.example`).
2. Run `make install` and `make install-dashboard` to set up dependencies.
3. Bring up Kafka:
```bash
docker-compose -f kafka/docker-compose.yml up -d
make topic
```
4. Initialise PostgreSQL:
```bash
make postgres-init
```
*(Optionally seed demo data to use the dashboard immediately without the pipeline: `make seed-data`)*

## Running the End-to-End Pipeline
Use three parallel terminal windows. Make sure your virtual environment is activated in each!

**Terminal 1 (Producer):**
```bash
python kafka/producer.py
```

**Terminal 2 (PySpark Streaming):**
```bash
python spark/streaming_job.py
```

**Terminal 3 (StreamPulse Dashboard):**
```bash
streamlit run dashboard/app.py
```

The live dashboard will be available at `http://localhost:8501`.

## Testing
Run the comprehensive test suite to validate schemas, producer specs, and transformations:
```bash
pytest tests/ -v
```

## Author
Ynez Dias

## License
MIT License
