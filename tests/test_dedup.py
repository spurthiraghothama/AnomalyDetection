"""
Tests for the two-phase deduplication claim.

The property under test is that an event is never marked complete before its
effects are durable. The original implementation used a single SET NX as both
the check and the claim, so an event whose database write failed was recorded
as handled and then skipped on redelivery, losing it for the length of the TTL.

Run with pytest, or directly:  python tests/test_dedup.py
"""

import os
import sys

import redis

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class FakeRedis:
    """Minimal in-memory stand-in. TTLs are accepted and ignored."""

    def __init__(self):
        self.store = {}
        self.fail = False

    def ping(self):
        return True

    def set(self, key, value, nx=False, ex=None):
        if self.fail:
            raise redis.RedisError("simulated outage")
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def get(self, key):
        if self.fail:
            raise redis.RedisError("simulated outage")
        return self.store.get(key)

    def delete(self, key):
        if self.fail:
            raise redis.RedisError("simulated outage")
        self.store.pop(key, None)
        return 1


def _deduplicator():
    """Builds a RedisDeduplicator backed by FakeRedis."""
    fake = FakeRedis()
    redis.from_url = lambda *args, **kwargs: fake
    from distributed.consumer_worker import RedisDeduplicator

    return RedisDeduplicator("redis://fake/0", ttl_seconds=3600), fake


def test_fresh_event_is_claimed():
    dedup, _ = _deduplicator()
    assert dedup.claim("evt-fresh") is True


def test_confirmed_event_is_skipped():
    dedup, _ = _deduplicator()
    dedup.claim("evt-done")
    dedup.confirm("evt-done")
    assert dedup.claim("evt-done") is False


def test_released_event_can_be_retried():
    """A failed event must be reprocessable immediately, not dropped."""
    dedup, _ = _deduplicator()
    dedup.claim("evt-failed")
    dedup.release("evt-failed")  # stands in for the DB insert failing
    assert dedup.claim("evt-failed") is True


def test_stale_pending_marker_reprocesses():
    """
    A node that dies between claim and confirm leaves a pending marker.
    Treating that as a duplicate would lose the event, so we reprocess and
    let the event_id UNIQUE constraint reject any double-write.
    """
    dedup, fake = _deduplicator()
    fake.store["event:evt-orphan"] = "pending:consumer-that-died"
    assert dedup.claim("evt-orphan") is True


def test_redis_outage_fails_open():
    """Losing Redis must degrade into duplicate work, never into data loss."""
    dedup, fake = _deduplicator()
    fake.fail = True
    assert dedup.claim("evt-no-redis") is True


def test_confirm_survives_redis_outage():
    """A failed confirm must not raise; the pending marker expires on its own."""
    dedup, fake = _deduplicator()
    dedup.claim("evt-x")
    fake.fail = True
    dedup.confirm("evt-x")  # must not raise


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError:
            print(f"FAIL  {name}")
            failures += 1
    sys.exit(1 if failures else 0)
