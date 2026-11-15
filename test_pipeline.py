import unittest

from pipeline import ReceiptStore, StageRunner


class FakeTimer:
    def __init__(self, interval, callback):
        self.callback = callback

    def start(self):
        pass

    def cancel(self):
        pass


class StageRunnerTest(unittest.TestCase):
    def test_finished_stage_writes_done(self):
        store = ReceiptStore()
        StageRunner(store, timer_factory=FakeTimer).run("run-1", lambda: None)
        self.assertEqual(store.all(), [("run-1", "done")])


if __name__ == "__main__":
    unittest.main()
