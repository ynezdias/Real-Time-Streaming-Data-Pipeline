# ── Real-Time Streaming Data Pipeline — Makefile ──────────────────────────
# Usage:  make <target>

.PHONY: help kafka-up kafka-down topic producer spark dashboard seed-data \
        postgres-init install install-dashboard test clean

# ── Default target ────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ⚡ StreamPulse — Real-Time Streaming Data Pipeline"
	@echo ""
	@echo "  Available targets:"
	@echo "    kafka-up          Start Kafka + Zookeeper via Docker Compose"
	@echo "    kafka-down        Stop and remove Kafka containers"
	@echo "    topic             Create the user-events Kafka topic"
	@echo "    producer          Run the Kafka event producer (500K events)"
	@echo "    spark             Submit the PySpark streaming job"
	@echo "    postgres-init     Initialise the PostgreSQL schema"
	@echo "    seed-data         Seed PostgreSQL with 7 days of demo data"
	@echo "    dashboard         Launch the Streamlit dashboard"
	@echo "    install           Install all Python dependencies"
	@echo "    install-dashboard Install dashboard-only dependencies"
	@echo "    test              Run pytest test suite"
	@echo "    clean             Remove __pycache__ and .pytest_cache"
	@echo ""

# ── Infrastructure ────────────────────────────────────────────────────────
kafka-up:
	docker-compose -f kafka/docker-compose.yml up -d
	@echo "✅  Kafka + Zookeeper started"

kafka-down:
	docker-compose -f kafka/docker-compose.yml down -v
	@echo "🛑  Kafka stopped"

topic:
	bash kafka/topics.sh
	@echo "✅  Topic 'user-events' created"

# ── Pipeline ──────────────────────────────────────────────────────────────
producer:
	python kafka/producer.py

spark:
	cd spark && spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		streaming_job.py

postgres-init:
	psql -h $${PG_HOST:-localhost} -U $${PG_USER:-postgres} \
	     -d $${PG_DB:-pipeline} -f storage/init_db.sql
	@echo "✅  PostgreSQL schema initialised"

# ── Dashboard ─────────────────────────────────────────────────────────────
seed-data:
	python dashboard/seed_data.py

dashboard:
	streamlit run dashboard/app.py \
		--server.port 8501 \
		--server.headless false \
		--browser.gatherUsageStats false

# ── Full demo (seeds data + launches dashboard) ───────────────────────────
demo: seed-data dashboard

# ── Dependencies ──────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

install-dashboard:
	pip install -r dashboard/requirements.txt

# ── Testing ───────────────────────────────────────────────────────────────
test:
	pytest tests/ -v

# ── Cleanup ───────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "🧹  Cleaned up"
