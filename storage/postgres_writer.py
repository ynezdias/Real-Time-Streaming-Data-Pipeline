import psycopg2
import os

def write_aggregates(rows):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        database=os.getenv("PG_DB", "pipeline"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD")
    )
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO event_aggregates
            (window_start, window_end, event_type, country, event_count, unique_users, total_revenue)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row.window_start, row.window_end, row.event_type,
              row.country, row.event_count, row.unique_users, row.total_revenue))
    conn.commit()
    cur.close()
    conn.close()