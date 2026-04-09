"""
seed_data.py — Populates PostgreSQL with realistic demo data so the
StreamPulse dashboard works without needing the live Kafka→Spark pipeline.

Run once before starting the dashboard:
    python dashboard/seed_data.py

Generates 7 days of 5-minute windowed aggregates with:
  - Realistic daytime traffic peaks (09:00–22:00)
  - Weekend uplift in purchases
  - Gaussian noise for natural variance
  - Proportional event-type ratios matching real e-commerce platforms
"""

import os
import random
import math
from datetime import datetime, timedelta

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DAYS            = 7           # days of history to generate
WINDOW_MINUTES  = 5           # matches Spark's 5-min tumbling windows
BASE_USERS      = 80          # average unique users per window at peak hour
COUNTRIES       = [
    "US", "GB", "DE", "FR", "CA", "AU", "IN", "BR", "JP", "MX",
    "ES", "IT", "NL", "SE", "SG", "KR", "PL", "TR", "ZA", "AR",
]
EVENT_TYPES     = ["page_view", "search", "add_to_cart", "purchase", "logout"]
# Probability weights per event type (must sum to 1)
EVENT_WEIGHTS   = [0.45,       0.25,     0.15,          0.10,       0.05]
# Only purchase events have revenue
AVG_ORDER_VALUE = 85.0


def _hour_multiplier(hour: int, is_weekend: bool) -> float:
    """Return a traffic multiplier based on hour of day and day type."""
    # Gaussian peak centred at 14:00 (2 PM), σ=4 hours
    peak = math.exp(-0.5 * ((hour - 14) / 4) ** 2)
    night_floor = 0.05
    base = night_floor + (1 - night_floor) * peak
    return base * (1.25 if is_weekend else 1.0)


def generate_rows() -> list[tuple]:
    """Yield (window_start, window_end, event_type, country, …) tuples."""
    rows = []
    now = datetime.utcnow().replace(second=0, microsecond=0)
    # Align to the nearest 5-min boundary
    now -= timedelta(minutes=now.minute % WINDOW_MINUTES)
    start_time = now - timedelta(days=DAYS)

    current = start_time
    while current < now:
        window_end = current + timedelta(minutes=WINDOW_MINUTES)
        is_weekend = current.weekday() >= 5
        multiplier = _hour_multiplier(current.hour, is_weekend)

        # Pick ~6 country/event-type combos per window (sparse, realistic)
        for _ in range(random.randint(4, 8)):
            country    = random.choice(COUNTRIES)
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

            unique_users = max(1, int(
                BASE_USERS * multiplier
                * random.gauss(1.0, 0.15)       # ±15% noise
                * (0.6 if country != "US" else 1.0)  # US gets more traffic
            ))
            # Events are roughly 3–8× the unique-user count
            event_count = max(unique_users, int(
                unique_users * random.uniform(3, 8)
            ))

            if event_type == "purchase":
                revenue = round(
                    event_count * AVG_ORDER_VALUE * random.gauss(1.0, 0.25), 2
                )
                revenue = max(0, revenue)
            else:
                revenue = 0.0

            rows.append((
                current, window_end,
                event_type, country,
                event_count, unique_users, revenue,
            ))

        current = window_end

    return rows


def seed(clear_existing: bool = True):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "password"),
    )
    cur = conn.cursor()

    # Ensure the table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS event_aggregates (
            id              SERIAL PRIMARY KEY,
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            event_type      VARCHAR(50),
            country         VARCHAR(10),
            event_count     INTEGER,
            unique_users    INTEGER,
            total_revenue   NUMERIC(12, 2),
            created_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_event_type   ON event_aggregates(event_type);
        CREATE INDEX IF NOT EXISTS idx_window_start ON event_aggregates(window_start);
        CREATE INDEX IF NOT EXISTS idx_country      ON event_aggregates(country);
    """)

    if clear_existing:
        cur.execute("TRUNCATE TABLE event_aggregates RESTART IDENTITY;")
        print("🗑  Cleared existing data.")

    rows = generate_rows()
    cur.executemany("""
        INSERT INTO event_aggregates
            (window_start, window_end, event_type, country,
             event_count, unique_users, total_revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅  Seeded {len(rows):,} rows covering {DAYS} days of history.")


if __name__ == "__main__":
    print("🌱  Seeding StreamPulse demo data …")
    seed(clear_existing=True)
