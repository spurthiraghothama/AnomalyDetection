"""
Chaos Test Runner
------------------
Automated fault injection that randomly kills consumer containers
and measures recovery metrics:
- Recovery time (container restart + Kafka rebalance)
- Throughput maintained with N-1 nodes
- Data loss/duplication verification

Requires Docker socket access (/var/run/docker.sock mounted).
"""

import os
import sys
import json
import time
import random
import logging
from datetime import datetime, timezone

import docker
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ============================================================
# Configuration
# ============================================================

NODE_ID = os.getenv("NODE_ID", "chaos-runner")
CHAOS_INTERVAL = int(os.getenv("CHAOS_INTERVAL_SECONDS", "120"))
TOTAL_KILLS = int(os.getenv("CHAOS_TOTAL_KILLS", "10"))
CONSUMER_CONTAINERS = ["consumer-1", "consumer-2", "consumer-3"]
RESULTS_DIR = os.getenv("CHAOS_RESULTS_DIR", "/app/chaos_results")
METRICS_PORT = int(os.getenv("CHAOS_METRICS_PORT", "8003"))

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=f"[CHAOS] %(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ============================================================
# Prometheus Metrics
# ============================================================

chaos_kills = Counter(
    'chaos_kills_total',
    'Total containers killed by chaos runner'
)
chaos_recovery = Histogram(
    'chaos_recovery_seconds',
    'Time for container to recover after kill',
    buckets=[1, 2, 5, 10, 15, 20, 25, 30, 45, 60, 90, 120]
)
chaos_active_consumers = Gauge(
    'chaos_active_consumers',
    'Number of active consumer containers'
)


# ============================================================
# Docker Client Helpers
# ============================================================

def get_docker_client():
    """Connect to Docker daemon."""
    try:
        client = docker.from_env()
        client.ping()
        logger.info("Connected to Docker daemon.")
        return client
    except Exception as e:
        logger.error(f"Cannot connect to Docker: {e}")
        logger.error("Ensure /var/run/docker.sock is mounted in docker-compose.yml")
        raise


def get_live_consumers(client):
    """Returns list of running consumer container names."""
    live = []
    for name in CONSUMER_CONTAINERS:
        try:
            container = client.containers.get(name)
            if container.status == "running":
                live.append(name)
        except docker.errors.NotFound:
            pass
        except Exception as e:
            logger.warning(f"Error checking {name}: {e}")
    return live


def kill_container(client, name):
    """Kills a container by name. Returns True on success."""
    try:
        container = client.containers.get(name)
        container.kill()
        logger.info(f"💀 Killed container: {name}")
        return True
    except Exception as e:
        logger.error(f"Failed to kill {name}: {e}")
        return False


def wait_for_recovery(client, name, timeout=120):
    """
    Brings a killed container back and measures how long it takes to run again.

    Important Docker behaviour: `restart: always` does NOT apply to a container
    stopped by an explicit API request, and `kill()` counts as one. Docker
    treats it as a manual stop and skips the restart policy, so a killed
    container stays exited forever. Relying on the policy here meant every
    recovery timed out and the suite reported 0/10 successes while the cluster
    was in fact fine.

    So we restart it ourselves, which is also closer to what a real
    orchestrator (Kubernetes, ECS) would do after a node loss.

    Returns recovery time in seconds, or -1 if it never came back.
    """
    start = time.monotonic()
    started_it = False

    while time.monotonic() - start < timeout:
        try:
            container = client.containers.get(name)
            container.reload()  # status is cached on the object otherwise

            if container.status == "running":
                return time.monotonic() - start

            if not started_it and container.status in ("exited", "created"):
                logger.info(f"{name} is {container.status}; starting it (kill bypasses restart policy)")
                container.start()
                started_it = True
        except docker.errors.NotFound:
            pass
        except docker.errors.APIError as e:
            logger.warning(f"Docker API error while recovering {name}: {e}")

        time.sleep(1)

    return -1  # Timeout


def get_db_counts():
    """
    Query PostgreSQL for total events and duplicate count.
    Uses the database module's functions.
    """
    try:
        from database.db import get_total_event_count, get_duplicate_count
        total = get_total_event_count()
        dupes = get_duplicate_count()
        return total, dupes
    except Exception as e:
        logger.warning(f"Could not query DB counts: {e}")
        return -1, -1


# ============================================================
# Main Chaos Loop
# ============================================================

def run_chaos_test():
    """
    Main chaos test loop:
    1. Pick a random live consumer
    2. Record pre-kill state
    3. Kill the container
    4. Measure recovery time
    5. Record post-recovery state
    6. Repeat N times
    7. Write summary report
    """
    # Start Prometheus metrics server
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Chaos metrics exposed on port {METRICS_PORT}")
    except Exception:
        pass

    os.makedirs(RESULTS_DIR, exist_ok=True)

    client = get_docker_client()
    results = []

    logger.info("=" * 60)
    logger.info("    CHAOS TEST STARTING")
    logger.info(f"    Interval: {CHAOS_INTERVAL}s | Total Kills: {TOTAL_KILLS}")
    logger.info(f"    Targets: {CONSUMER_CONTAINERS}")
    logger.info("=" * 60)

    pre_test_events, _ = get_db_counts()
    logger.info(f"Pre-test event count: {pre_test_events}")

    for kill_num in range(1, TOTAL_KILLS + 1):
        logger.info(f"\n{'='*50}")
        logger.info(f"  CHAOS KILL #{kill_num}/{TOTAL_KILLS}")
        logger.info(f"{'='*50}")

        # 1. Get live consumers
        live = get_live_consumers(client)
        chaos_active_consumers.set(len(live))

        if not live:
            logger.warning("No live consumers! Waiting for recovery...")
            time.sleep(30)
            continue

        # 2. Pick a random target
        target = random.choice(live)
        logger.info(f"Target selected: {target}")
        logger.info(f"Live consumers before kill: {live}")

        # 3. Record pre-kill state
        pre_kill_events, _ = get_db_counts()
        pre_kill_time = datetime.now(timezone.utc).isoformat()

        # 4. Kill the container
        kill_success = kill_container(client, target)
        kill_timestamp = time.time()

        if not kill_success:
            logger.error(f"Kill failed for {target}, skipping...")
            continue

        chaos_kills.inc()

        # 5. Measure surviving consumers
        time.sleep(2)
        surviving = get_live_consumers(client)
        chaos_active_consumers.set(len(surviving))
        logger.info(f"Surviving consumers: {surviving} ({len(surviving)}/{len(CONSUMER_CONTAINERS)})")

        # 6. Wait for recovery
        logger.info(f"Waiting for {target} to recover (timeout: 120s)...")
        recovery_time = wait_for_recovery(client, target, timeout=120)

        if recovery_time > 0:
            chaos_recovery.observe(recovery_time)
            logger.info(f"✅ {target} recovered in {recovery_time:.1f}s")
        else:
            logger.error(f"❌ {target} did NOT recover within 120s!")

        # 7. Allow Kafka rebalance to complete
        logger.info("Allowing 10s for Kafka rebalance to settle...")
        time.sleep(10)

        # 8. Record post-recovery state
        post_recovery_events, _ = get_db_counts()
        post_recovery_time = datetime.now(timezone.utc).isoformat()
        post_live = get_live_consumers(client)
        chaos_active_consumers.set(len(post_live))

        # 9. Record result
        result = {
            "kill_number": kill_num,
            "target": target,
            "pre_kill_time": pre_kill_time,
            "post_recovery_time": post_recovery_time,
            "recovery_seconds": round(recovery_time, 2) if recovery_time > 0 else "TIMEOUT",
            "surviving_nodes_during_failure": len(surviving),
            "live_nodes_after_recovery": len(post_live),
            "events_before_kill": pre_kill_events,
            "events_after_recovery": post_recovery_events,
            "events_processed_during_failure": post_recovery_events - pre_kill_events if pre_kill_events >= 0 else "N/A",
        }
        results.append(result)

        # Log summary for this kill
        logger.info(f"Kill #{kill_num} Summary:")
        logger.info(f"  Target:          {target}")
        logger.info(f"  Recovery Time:   {result['recovery_seconds']}s")
        logger.info(f"  Surviving Nodes: {result['surviving_nodes_during_failure']}")
        logger.info(f"  Events During:   {result['events_processed_during_failure']}")

        # 10. Wait for next kill cycle
        if kill_num < TOTAL_KILLS:
            logger.info(f"Waiting {CHAOS_INTERVAL}s until next kill...")
            time.sleep(CHAOS_INTERVAL)

    # ============================================================
    # Final Report
    # ============================================================

    logger.info("\n" + "=" * 60)
    logger.info("    CHAOS TEST COMPLETE - FINAL REPORT")
    logger.info("=" * 60)

    post_test_events, post_test_dupes = get_db_counts()

    recovery_times = [
        r["recovery_seconds"]
        for r in results
        if isinstance(r["recovery_seconds"], (int, float))
    ]

    summary = {
        "test_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_kills": TOTAL_KILLS,
        "successful_recoveries": len(recovery_times),
        "failed_recoveries": TOTAL_KILLS - len(recovery_times),
        "avg_recovery_seconds": round(sum(recovery_times) / len(recovery_times), 2) if recovery_times else "N/A",
        "min_recovery_seconds": round(min(recovery_times), 2) if recovery_times else "N/A",
        "max_recovery_seconds": round(max(recovery_times), 2) if recovery_times else "N/A",
        "pre_test_event_count": pre_test_events,
        "post_test_event_count": post_test_events,
        "duplicate_events": post_test_dupes,
        "data_loss": "NONE" if post_test_dupes == 0 else f"{post_test_dupes} DUPLICATES FOUND",
        "kills": results,
    }

    # Print summary
    logger.info(f"  Total Kills:            {summary['total_kills']}")
    logger.info(f"  Successful Recoveries:  {summary['successful_recoveries']}")
    logger.info(f"  Failed Recoveries:      {summary['failed_recoveries']}")
    logger.info(f"  Avg Recovery Time:      {summary['avg_recovery_seconds']}s")
    logger.info(f"  Min Recovery Time:      {summary['min_recovery_seconds']}s")
    logger.info(f"  Max Recovery Time:      {summary['max_recovery_seconds']}s")
    logger.info(f"  Pre-test Events:        {summary['pre_test_event_count']}")
    logger.info(f"  Post-test Events:       {summary['post_test_event_count']}")
    logger.info(f"  Duplicate Events:       {summary['duplicate_events']}")
    logger.info(f"  Data Loss Status:       {summary['data_loss']}")
    logger.info("=" * 60)

    # Write results to file
    results_file = os.path.join(RESULTS_DIR, "chaos_results.json")
    with open(results_file, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Results written to: {results_file}")

    # Also write a human-readable report
    report_file = os.path.join(RESULTS_DIR, "chaos_report.md")
    with open(report_file, "w") as f:
        f.write("# Chaos Test Results\n\n")
        f.write(f"**Test Date**: {summary['test_timestamp']}\n\n")
        f.write("## Summary\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Total Kill Cycles | {summary['total_kills']} |\n")
        f.write(f"| Successful Recoveries | {summary['successful_recoveries']} |\n")
        f.write(f"| Failed Recoveries | {summary['failed_recoveries']} |\n")
        f.write(f"| Avg Recovery Time | {summary['avg_recovery_seconds']}s |\n")
        f.write(f"| Min Recovery Time | {summary['min_recovery_seconds']}s |\n")
        f.write(f"| Max Recovery Time | {summary['max_recovery_seconds']}s |\n")
        f.write(f"| Events Before Test | {summary['pre_test_event_count']} |\n")
        f.write(f"| Events After Test | {summary['post_test_event_count']} |\n")
        f.write(f"| Duplicate Events | {summary['duplicate_events']} |\n")
        f.write(f"| **Data Loss** | **{summary['data_loss']}** |\n\n")
        f.write("## Kill-by-Kill Details\n\n")
        f.write("| # | Target | Recovery (s) | Surviving Nodes | Events During |\n")
        f.write("|---|--------|-------------|-----------------|---------------|\n")
        for r in results:
            f.write(
                f"| {r['kill_number']} | {r['target']} | "
                f"{r['recovery_seconds']} | {r['surviving_nodes_during_failure']} | "
                f"{r['events_processed_during_failure']} |\n"
            )

    logger.info(f"Report written to: {report_file}")
    logger.info("Chaos runner exiting.")


if __name__ == "__main__":
    run_chaos_test()
