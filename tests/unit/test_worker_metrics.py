import subprocess
import sys

READ_A_NEW_WORKERS_COUNTERS = """
from prometheus_client import REGISTRY
import webhook_service.worker.metrics
for name, labels in [
    ("webhooks_deliveries_finished_total", {"status": "failed"}),
    ("webhooks_worker_errors_total", {"kind": "unexpected"}),
    ("webhooks_attempts_total", {"outcome": "retryable_failure", "error_category": "timeout"}),
]:
    print(REGISTRY.get_sample_value(name, labels))
"""


def test_a_new_worker_starts_its_counters_at_zero_so_the_first_failure_counts():
    # In a process of its own, as a worker is after a restart. A counter that appeared only
    # at its first increment would start at 1, and increase() would not count that 1.
    result = subprocess.run(
        [sys.executable, "-c", READ_A_NEW_WORKERS_COUNTERS],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.split() == ["0.0", "0.0", "0.0"]
