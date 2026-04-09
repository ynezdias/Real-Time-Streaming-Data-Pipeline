"""
queries.py — Efficient SQL query layer for the StreamPulse dashboard.

All queries use indexed columns (event_type, window_start) and parameterized
inputs to prevent SQL injection. Connection is cached via st.cache_resource
to avoid reconnecting on every Streamlit rerun.
"""

import os
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Time window → PostgreSQL INTERVAL mapping
# ---------------------------------------------------------------------------
INTERVAL_MAP = {
    "Last 1 Hour":   "1 hour",
    "Last 6 Hours":  "6 hours",
    "Last 24 Hours": "24 hours",
    "Last 7 Days":   "7 days",
    "Last 30 Days":  "30 days",
}

# ---------------------------------------------------------------------------
# Cached connection (one connection per Streamlit session)
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_connection():
    """Return a persistent psycopg2 connection, cached per session."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "password"),
    )


def _query_df(sql: str, params: tuple = None) -> pd.DataFrame:
    """
    Execute a SELECT query and return a DataFrame.
    Handles connection drops by clearing the cache and reconnecting once.
    Returns an empty DataFrame on error so the dashboard degrades gracefully.
    """
    try:
        conn = get_connection()
        # Reconnect if the connection was closed (e.g. Postgres restarted)
        if conn.closed:
            get_connection.clear()
            conn = get_connection()
        return pd.read_sql_query(sql, conn, params=params)
    except psycopg2.OperationalError:
        # Force a fresh connection next call
        get_connection.clear()
        return pd.DataFrame()
    except Exception as exc:
        st.warning(f"⚠️  Query error: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

def get_filter_options() -> tuple[list, list]:
    """Return (event_types, countries) lists prefixed with 'All'."""
    et_df = _query_df(
        "SELECT DISTINCT event_type FROM event_aggregates ORDER BY event_type"
    )
    co_df = _query_df(
        "SELECT DISTINCT country FROM event_aggregates ORDER BY country"
    )
    event_types = ["All"] + (et_df["event_type"].tolist() if not et_df.empty else [])
    countries   = ["All"] + (co_df["country"].tolist()    if not co_df.empty else [])
    return event_types, countries


def get_kpi_summary(
    time_window: str,
    event_type: str = "All",
    country: str = "All",
) -> pd.DataFrame:
    """
    Aggregate KPIs for the selected filters:
        total_events, total_unique_users, total_revenue, active_countries
    Uses indexed columns: window_start, event_type.
    """
    interval = INTERVAL_MAP.get(time_window, "24 hours")
    sql = """
        SELECT
            COALESCE(SUM(event_count),   0)::BIGINT   AS total_events,
            COALESCE(SUM(unique_users),  0)::BIGINT   AS total_unique_users,
            COALESCE(SUM(total_revenue), 0)::NUMERIC  AS total_revenue,
            COUNT(DISTINCT country)::INT              AS active_countries
        FROM event_aggregates
        WHERE window_start >= NOW() - INTERVAL %s
          AND (%s = 'All' OR event_type = %s)
          AND (%s = 'All' OR country    = %s)
    """
    return _query_df(sql, (interval, event_type, event_type, country, country))


def get_events_over_time(
    time_window: str,
    event_type: str = "All",
    country: str = "All",
) -> pd.DataFrame:
    """
    Hourly bucketed time-series of events, unique users, and revenue.
    Columns: hour, event_count, unique_users, revenue
    """
    interval = INTERVAL_MAP.get(time_window, "24 hours")
    # Bucket size adapts to the selected window for readability
    bucket = "10 minutes" if time_window == "Last 1 Hour" else \
             "30 minutes" if time_window == "Last 6 Hours" else \
             "1 hour"     if time_window in ("Last 24 Hours", "Last 7 Days") else \
             "6 hours"
    sql = f"""
        SELECT
            date_trunc('minute',
                floor(extract(epoch FROM window_start)
                    / extract(epoch FROM INTERVAL '{bucket}'))
                * INTERVAL '{bucket}'
            )                                                   AS bucket,
            SUM(event_count)::BIGINT                            AS event_count,
            SUM(unique_users)::BIGINT                           AS unique_users,
            COALESCE(SUM(total_revenue), 0)::NUMERIC            AS revenue
        FROM event_aggregates
        WHERE window_start >= NOW() - INTERVAL %s
          AND (%s = 'All' OR event_type = %s)
          AND (%s = 'All' OR country    = %s)
        GROUP BY 1
        ORDER BY 1
    """
    return _query_df(sql, (interval, event_type, event_type, country, country))


def get_event_type_distribution(
    time_window: str,
    country: str = "All",
) -> pd.DataFrame:
    """
    Per-event-type aggregated counts, users, and revenue.
    Columns: event_type, event_count, unique_users, revenue
    """
    interval = INTERVAL_MAP.get(time_window, "24 hours")
    sql = """
        SELECT
            event_type,
            SUM(event_count)::BIGINT                  AS event_count,
            SUM(unique_users)::BIGINT                 AS unique_users,
            COALESCE(SUM(total_revenue), 0)::NUMERIC  AS revenue
        FROM event_aggregates
        WHERE window_start >= NOW() - INTERVAL %s
          AND (%s = 'All' OR country = %s)
        GROUP BY event_type
        ORDER BY event_count DESC
    """
    return _query_df(sql, (interval, country, country))


def get_top_countries(
    time_window: str,
    event_type: str = "All",
    n: int = 10,
) -> pd.DataFrame:
    """
    Top-N countries by total event count.
    Columns: country, event_count, revenue
    """
    interval = INTERVAL_MAP.get(time_window, "24 hours")
    sql = """
        SELECT
            country,
            SUM(event_count)::BIGINT                  AS event_count,
            COALESCE(SUM(total_revenue), 0)::NUMERIC  AS revenue
        FROM event_aggregates
        WHERE window_start >= NOW() - INTERVAL %s
          AND (%s = 'All' OR event_type = %s)
        GROUP BY country
        ORDER BY event_count DESC
        LIMIT %s
    """
    return _query_df(sql, (interval, event_type, event_type, n))


def get_purchase_trends(
    time_window: str,
    country: str = "All",
) -> pd.DataFrame:
    """
    Hourly purchase revenue and purchase event count.
    Columns: hour, revenue, purchases
    """
    interval = INTERVAL_MAP.get(time_window, "24 hours")
    sql = """
        SELECT
            date_trunc('hour', window_start)           AS hour,
            COALESCE(SUM(total_revenue), 0)::NUMERIC   AS revenue,
            SUM(event_count)::BIGINT                   AS purchases
        FROM event_aggregates
        WHERE window_start >= NOW() - INTERVAL %s
          AND event_type = 'purchase'
          AND (%s = 'All' OR country = %s)
        GROUP BY 1
        ORDER BY 1
    """
    return _query_df(sql, (interval, country, country))


def get_live_feed(limit: int = 20) -> pd.DataFrame:
    """
    Most recent aggregation windows — simulates a live activity feed.
    Columns: window_start, event_type, country, event_count, unique_users, total_revenue
    """
    sql = """
        SELECT
            window_start,
            event_type,
            country,
            event_count,
            unique_users,
            COALESCE(total_revenue, 0)::NUMERIC AS total_revenue
        FROM event_aggregates
        ORDER BY window_start DESC
        LIMIT %s
    """
    return _query_df(sql, (limit,))
