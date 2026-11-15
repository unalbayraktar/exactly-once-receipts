"""A pipeline stage guarded by a watchdog timer.

Each run must end with exactly one receipt, either "done" or "timeout".
"""
import threading


class ReceiptStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._receipts = []

    def has(self, run_id):
        with self._lock:
            return any(r[0] == run_id for r in self._receipts)

    def write(self, run_id, outcome):
        with self._lock:
            self._receipts.append((run_id, outcome))

    def all(self):
        with self._lock:
            return list(self._receipts)


class StageRunner:
    def __init__(self, store, timer_factory=threading.Timer, timeout=30.0):
        self._store = store
        self._timer_factory = timer_factory
        self._timeout = timeout

    def run(self, run_id, work):
        timer = self._timer_factory(self._timeout, lambda: self._on_timeout(run_id))
        timer.start()
        try:
            work()
        finally:
            timer.cancel()
        if not self._store.has(run_id):
            self._store.write(run_id, "done")

    def _on_timeout(self, run_id):
        if not self._store.has(run_id):
            self._store.write(run_id, "timeout")
