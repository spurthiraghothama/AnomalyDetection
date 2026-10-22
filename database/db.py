import os
import sqlite3
from contextlib import contextmanager
from dotenv import load_dotenv

# Load configurations
load_dotenv()

# Check if we're running in distributed mode (PostgreSQL) or local mode (SQLite)
DATABASE_URL = os.getenv("DATABASE_URL", "")

# SQLite fallback path for local development
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anomalies.db')
DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)

# If DB_PATH is specified as a relative path in .env, make it absolute relative to project root
if not os.path.isabs(DB_PATH):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB_PATH = os.path.abspath(os.path.join(project_root, DB_PATH))

# Determine backend
_use_postgres = bool(DATABASE_URL)
_pg_pool = None


def _init_postgres_pool():
    """Initialize a thread-safe PostgreSQL connection pool."""
    global _pg_pool
    if _pg_pool is not None:
        return
    try:
        import psycopg2
        from psycopg2 import pool as pg_pool
        _pg_pool = pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL
        )
        print(f"[Database] PostgreSQL connection pool initialized: {DATABASE_URL.split('@')[-1]}")
    except Exception as e:
        print(f"[Database] ERROR initializing PostgreSQL pool: {e}")
        raise


@contextmanager
def get_connection():
    """
    Returns a database connection. Uses PostgreSQL if DATABASE_URL is set,
    otherwise falls back to SQLite for local development.
    """
    if _use_postgres:
        if _pg_pool is None:
            _init_postgres_pool()
        conn = _pg_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _pg_pool.putconn(conn)
    else:
        # SQLite fallback
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def init_db():
    """
    Creates the metrics table if it does not already exist.
    Supports both PostgreSQL and SQLite syntax.
    """
    if _use_postgres:
        query = """
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            timestamp TEXT NOT NULL,
            cpu REAL NOT NULL,
            memory REAL NOT NULL,
            latency REAL NOT NULL,
            error_rate REAL NOT NULL,
            is_anomaly INTEGER NOT NULL,
            anomaly_score REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_metrics_event_id ON metrics(event_id);
        CREATE INDEX IF NOT EXISTS idx_metrics_is_anomaly ON metrics(is_anomaly);
        """
    else:
        query = """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            timestamp TEXT NOT NULL,
            cpu REAL NOT NULL,
            memory REAL NOT NULL,
            latency REAL NOT NULL,
            error_rate REAL NOT NULL,
            is_anomaly INTEGER NOT NULL,
            anomaly_score REAL NOT NULL
        );
        """

    with get_connection() as conn:
        cursor = conn.cursor()
        if _use_postgres:
            # PostgreSQL supports multiple statements in execute
            cursor.execute(query)
        else:
            cursor.executescript(query)
        conn.commit()

    backend = f"PostgreSQL ({DATABASE_URL.split('@')[-1]})" if _use_postgres else f"SQLite ({DB_PATH})"
    print(f"[Database] Initialized on: {backend}")


def insert_metric(timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score, event_id=None):
    """
    Inserts a scored metrics data point into the database.
    Uses ON CONFLICT DO NOTHING for idempotent inserts when event_id is provided.
    """
    is_anomaly_int = 1 if is_anomaly else 0

    if event_id:
        if _use_postgres:
            query = """
            INSERT INTO metrics (event_id, timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
            """
        else:
            query = """
            INSERT OR IGNORE INTO metrics (event_id, timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
        params = (event_id, timestamp, cpu, memory, latency, error_rate, is_anomaly_int, anomaly_score)
    else:
        # Legacy path (no event_id) for backward compatibility with local consumer
        if _use_postgres:
            query = """
            INSERT INTO metrics (timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
        else:
            query = """
            INSERT INTO metrics (timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """
        params = (timestamp, cpu, memory, latency, error_rate, is_anomaly_int, anomaly_score)

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()


def get_recent_metrics(n=100):
    """
    Fetches the last n recorded metrics.
    Returns a list of dictionaries.
    """
    if _use_postgres:
        query = """
        SELECT timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score 
        FROM metrics 
        ORDER BY id DESC 
        LIMIT %s;
        """
    else:
        query = """
        SELECT timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score 
        FROM metrics 
        ORDER BY id DESC 
        LIMIT ?;
        """

    with get_connection() as conn:
        if _use_postgres:
            import psycopg2.extras
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

        cursor.execute(query, (n,))
        rows = cursor.fetchall()

        results = [dict(row) for row in rows]
        results.reverse()
        return results


def get_anomalies_only():
    """
    Fetches all logged anomalies (where is_anomaly = 1).
    Ordered newest first for easy log displays.
    """
    query = """
    SELECT timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score 
    FROM metrics 
    WHERE is_anomaly = 1 
    ORDER BY id DESC;
    """
    with get_connection() as conn:
        if _use_postgres:
            import psycopg2.extras
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


def get_total_event_count():
    """
    Returns the total number of events in the database.
    Used for data loss verification in chaos testing.
    """
    query = "SELECT COUNT(*) FROM metrics;"
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]


def get_duplicate_count():
    """
    Returns the count of duplicate event_ids (should be 0 if idempotency works).
    """
    query = """
    SELECT COUNT(*) FROM (
        SELECT event_id FROM metrics 
        WHERE event_id IS NOT NULL
        GROUP BY event_id 
        HAVING COUNT(*) > 1
    ) AS dupes;
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]


# Initialize the database immediately when this module is imported
init_db()

# Optional verification block to test database functionality independently
if __name__ == "__main__":
    print("Testing database/db.py functions...")
    from datetime import datetime
    
    # 1. Test insertion
    test_time = datetime.now().isoformat()
    insert_metric(test_time, 45.2, 51.5, 120.4, 0.015, False, -0.125, event_id="test-001")
    insert_metric(test_time, 95.0, 88.0, 1500.0, 0.450, True, 0.285, event_id="test-002")
    print("[Database Test] Inserted two test metric records.")

    # 2. Test idempotent insert (same event_id should be ignored)
    insert_metric(test_time, 99.0, 99.0, 9999.0, 0.999, True, 0.999, event_id="test-001")
    print("[Database Test] Attempted duplicate insert (should be silently ignored).")
    
    # 3. Test fetching recent metrics
    recent = get_recent_metrics(n=5)
    print(f"[Database Test] Fetched {len(recent)} recent records:")
    for r in recent:
        print(f"  - {r['timestamp']} | CPU: {r['cpu']}% | Is Anomaly: {r['is_anomaly']}")
        
    # 4. Test fetching anomalies only
    anomalies = get_anomalies_only()
    print(f"[Database Test] Total anomalies logged: {len(anomalies)}")
    if anomalies:
        print(f"  - Latest anomaly: {anomalies[0]['timestamp']} | CPU: {anomalies[0]['cpu']}% | Score: {anomalies[0]['anomaly_score']}")

    # 5. Test counts
    print(f"[Database Test] Total events: {get_total_event_count()}")
    print(f"[Database Test] Duplicate event_ids: {get_duplicate_count()}")
