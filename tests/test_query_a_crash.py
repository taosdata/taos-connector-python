import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CRASH = os.path.join(HERE, "async_query_crash.py")


def test_async_query_does_not_crash():
    proc = subprocess.run(
        [sys.executable, CRASH],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=120,
    )
    output = proc.stdout.decode("utf-8", "replace")

    assert proc.returncode == 0
    assert "SURVIVED" in output
