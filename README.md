# exactly-once-receipts

A small pipeline stage with a watchdog timer. Every run must end with exactly one receipt, either `done` or `timeout`.

Run the tests with `python -m unittest -v`. There are no dependencies.
