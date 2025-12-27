import sys
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))
from liteflow import Dag, init_schema


def start_task():
    print("Task 1: Start")


def process_task():
    print("Task 2: Processing")


def end_task():
    print("Task 3: End")


def main():
    """Define and run a simple linear DAG without data passing."""
    db_path = "examples.db"
    init_schema(db_path)

    with Dag("simple_dag", db_path=db_path) as dag:
        # Register tasks
        t1 = dag.task(start_task)
        t2 = dag.task(process_task)
        t3 = dag.task(end_task)

        # Define workflow: Start -> Process -> End
        t1 >> t2 >> t3

    print("Running simple DAG...")
    dag.run()


if __name__ == "__main__":
    main()
