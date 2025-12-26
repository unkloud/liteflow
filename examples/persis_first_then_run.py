import os
import sys
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))

from liteflow import Dag, init_schema

DB_PATH = "persist_example.db"
DAG_ID = "two_stage_dag"


def step_1():
    print("Executing Step 1")
    return "Hello from Step 1"


def step_2(data):
    print(f"Executing Step 2. Received: {data}")
    return "Step 2 Complete"


def define_and_persist():
    print("--- Stage 1: Define and Persist ---")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_schema(DB_PATH)

    # The context manager automatically persists the DAG metadata upon exit
    with Dag(DAG_ID, db_path=DB_PATH, description="A DAG defined in stage 1") as dag:
        # We define tasks here. In a real scenario, this ensures the DAG structure is valid.
        # Note: LiteFlow does not store task code in the DB, only metadata.
        t1 = dag.task(step_1, task_id="step_1")
        t2 = dag.task(step_2, task_id="step_2", data=t1)

        print(f"DAG '{DAG_ID}' defined with tasks: {list(dag.tasks.keys())}")

    print("DAG metadata persisted to database.")


def load_and_run():
    print("\n--- Stage 2: Load and Run ---")
    # Load the DAG metadata from the database
    dag = Dag.load(DB_PATH, DAG_ID)
    print(f"Loaded DAG ID: {dag.dag_id}")
    print(f"Description: {dag.description}")

    # Since LiteFlow does not store code, we must re-attach the tasks to the loaded DAG object.
    # This allows us to use the DAG ID and settings from the DB, but run the current code.
    print("Re-attaching tasks for execution...")
    t1 = dag.task(step_1, task_id="step_1")
    t2 = dag.task(step_2, task_id="step_2", data=t1)

    print("Running DAG...")
    run = dag.run()
    print(f"Run ID: {run.run_id}")
    print(f"Status: {run.status}")


if __name__ == "__main__":
    define_and_persist()
    load_and_run()
