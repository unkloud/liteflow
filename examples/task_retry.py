import os
import sys
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))

from liteflow import Dag, init_schema

DB_PATH = "examples.db"
DAG_ID = "task_retry_example"
FLAG_FILE = "should_fail.flag"


def flaky_task():
    """
    Simulates a task that fails on the first attempt but succeeds on the second.
    Uses a file marker to track state across processes.
    """
    if os.path.exists(FLAG_FILE):
        print(f"[Task] Found {FLAG_FILE}, simulating failure...")
        os.remove(FLAG_FILE)
        raise Exception("Simulated transient error!")

    print("[Task] No flag file found. Succeeding.")
    return "Success"


def main():
    """Demonstrate task retries."""
    # 1. Setup
    init_schema(DB_PATH)

    # Create a flag file so the task fails the first time
    with open(FLAG_FILE, "w") as f:
        f.write("1")
    print(f"Created flag file '{FLAG_FILE}' to force initial failure.")

    try:
        with Dag(DAG_ID, db_path=DB_PATH) as dag:
            # Register a task with retry configuration
            # retries=2 means: 1 initial attempt + 2 retries = 3 total attempts max
            dag.task(
                flaky_task,
                task_id="flaky_step",
                retries=2,
                retry_delay=1,  # Wait 1 second before retrying
            )

        print("Running DAG with retry logic...")
        run = dag.run()
        print(f"Run finished with status: {run.status}")

    finally:
        # Cleanup in case the task didn't run or something else happened
        if os.path.exists(FLAG_FILE):
            os.remove(FLAG_FILE)


if __name__ == "__main__":
    main()
