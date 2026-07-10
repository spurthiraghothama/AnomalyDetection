import os
import sqlite3
from dotenv import load_dotenv

# Load configurations
load_dotenv()

# Resolve absolute path for database file to prevent multi-location database creation
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anomalies.db')
DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)

# If DB_PATH is specified as a relative path in .env, make it absolute relative to project root
if not os.path.isabs(DB_PATH):
    # Project root is one level up from database directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB_PATH = os.path.abspath(os.path.join(project_root, DB_PATH))

# Ensure the parent directory for the database exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def get_connection():
    """
    Establishes and returns a connection to the SQLite database.
    """
    return sqlite3.connect(DB_PATH)

def init_db():
    """
    Creates the metrics table if it does not already exist.
    """
    query = """
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        cpu REAL NOT NULL,
        memory REAL NOT NULL,
        latency REAL NOT NULL,
        error_rate REAL NOT NULL,
        is_anomaly INTEGER NOT NULL, -- 0 for false, 1 for true
        anomaly_score REAL NOT NULL
    );
    """
    with get_connection() as conn:
        conn.execute(query)
        conn.commit()
    print(f"[Database] SQLite database initialized at: {DB_PATH}")

def insert_metric(timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score):
    """
    Inserts a scored metrics data point into the database.
    """
    query = """
    INSERT INTO metrics (timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    # Convert boolean to integer for SQLite compatibility
    is_anomaly_int = 1 if is_anomaly else 0
    
    with get_connection() as conn:
        conn.execute(query, (timestamp, cpu, memory, latency, error_rate, is_anomaly_int, anomaly_score))
        conn.commit()

def get_recent_metrics(n=100):
    """
    Fetches the last n recorded metrics.
    Returns a list of dictionaries.
    """
    query = """
    SELECT timestamp, cpu, memory, latency, error_rate, is_anomaly, anomaly_score 
    FROM metrics 
    ORDER BY id DESC 
    LIMIT ?;
    """
    with get_connection() as conn:
        # Enable sqlite3.Row to easily convert results to a list of dicts
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, (n,))
        rows = cursor.fetchall()
        
        # Convert to dictionary format and reverse so they are chronological (oldest to newest)
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
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

# Initialize the database immediately when this module is imported
init_db()

# Optional verification block to test database functionality independently
if __name__ == "__main__":
    print("Testing database/db.py functions...")
    from datetime import datetime
    
    # 1. Test insertion
    test_time = datetime.now().isoformat()
    insert_metric(test_time, 45.2, 51.5, 120.4, 0.015, False, -0.125)
    insert_metric(test_time, 95.0, 88.0, 1500.0, 0.450, True, 0.285)
    print("[Database Test] Inserted two test metric records.")
    
    # 2. Test fetching recent metrics
    recent = get_recent_metrics(n=5)
    print(f"[Database Test] Fetched {len(recent)} recent records:")
    for r in recent:
        print(f"  - {r['timestamp']} | CPU: {r['cpu']}% | Is Anomaly: {r['is_anomaly']}")
        
    # 3. Test fetching anomalies only
    anomalies = get_anomalies_only()
    print(f"[Database Test] Total anomalies logged: {len(anomalies)}")
    if anomalies:
        print(f"  - Latest anomaly: {anomalies[0]['timestamp']} | CPU: {anomalies[0]['cpu']}% | Score: {anomalies[0]['anomaly_score']}")
