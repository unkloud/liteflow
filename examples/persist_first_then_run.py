import sys
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))

from liteflow import Dag, init_schema

DB_PATH = "examples.db"
DAG_ID = "lifecycle_dag"


def step_one():
    print("Executing Step 1")
    return "Hello from Step 1"


def step_two(input_data):
    print(f"Executing Step 2. Received: {input_data}")
    return "Step 2 Complete"


def define_and_persist():
    """Stage 1: Define DAG structure and persist to DB."""
    print("--- Stage 1: Define and Persist ---")
    init_schema(DB_PATH)

    # The context manager persists the DAG metadata (tasks, dependencies) upon exit.
    with Dag(DAG_ID, db_path=DB_PATH, description="Demonstrates persist/load lifecycle") as dag:
        t1 = dag.task(step_one, task_id="step_1")
        
        # Explicitly map the output of t1 to the 'input_data' argument of step_two
        t2 = dag.task(step_two, task_id="step_2", input_data=t1)

        print(f"Defined tasks: {list(dag.tasks.keys())}")
    
    print("DAG metadata persisted to database.")


def load_and_run():
    """Stage 2: Load DAG metadata and attach code for execution."""
    print("\n--- Stage 2: Load and Run ---")
    
    # 1. Load the DAG configuration from the database
    dag = Dag.load(DB_PATH, DAG_ID)
    print(f"Loaded DAG: {dag.dag_id} ({dag.description})")

    # 2. Re-attach the callable functions to the tasks.
    # LiteFlow stores the DAG structure (JSON/DB) but not the Python code.
    # We must bind the functions to the loaded task IDs.
    print("Re-attaching task functions...")
    
    t1 = dag.task(step_one, task_id="step_1")
    t2 = dag.task(step_two, task_id="step_2", input_data=t1)

    # 3. Execute
    print("Running DAG...")
    run = dag.run()
    print(f"Run ID: {run.run_id}, Status: {run.status}")


if __name__ == "__main__":
    define_and_persist()
    load_and_run()
