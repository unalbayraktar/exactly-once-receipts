import threading
import unittest

from pipeline import ReceiptStore, StageRunner


class FakeTimer:
    def __init__(self, interval, callback):
        self.callback = callback

    def start(self):
        pass

    def cancel(self):
        pass


class BlockingStore(ReceiptStore):
    """Blocks the first write until the test releases it."""

    def __init__(self):
        super().__init__()
        self.first_write_reached = threading.Event()
        self.release_first_write = threading.Event()
        self._calls = 0
        self._calls_lock = threading.Lock()

    def write(self, run_id, outcome):
        with self._calls_lock:
            self._calls += 1
            first = self._calls == 1
        if first:
            self.first_write_reached.set()
            assert self.release_first_write.wait(5)
        super().write(run_id, outcome)


class StageRunnerTest(unittest.TestCase):
    def test_finished_stage_writes_done(self):
        store = ReceiptStore()
        StageRunner(store, timer_factory=FakeTimer).run("run-1", lambda: None)
        self.assertEqual(store.all(), [("run-1", "done")])

    def test_stage_finishing_as_timer_fires_writes_one_receipt(self):
        store = BlockingStore()
        timers = []

        def timer_factory(interval, callback):
            timer = FakeTimer(interval, callback)
            timers.append(timer)
            return timer

        runner = StageRunner(store, timer_factory=timer_factory)
        stage = threading.Thread(target=runner.run, args=("run-1", lambda: None))
        stage.start()

        # The stage has decided to write "done" but has not written it yet.
        self.assertTrue(store.first_write_reached.wait(5))

        # The watchdog fires in that exact window and runs to completion.
        timers[0].callback()

        store.release_first_write.set()
        stage.join(5)
        self.assertFalse(stage.is_alive())

        self.assertEqual(len(store.all()), 1, store.all())


if __name__ == "__main__":
    unittest.main()
