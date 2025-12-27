import sys
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))
from typing import Dict
from liteflow import Dag, init_schema


def extract() -> Dict[str, int]:
    """Returns data to be used by downstream tasks."""
    print("Extracting data...")
    return {"value": 42}


def transform(extract: Dict[str, int]) -> int:
    """
    Receives output from 'extract' task.
    The argument name 'extract' matches the upstream task ID.
    """
    print(f"Transforming data: {extract}")
    return extract["value"] * 2


def load(transform: int):
    """Receives output from 'transform' task."""
    print(f"Loading data: {transform}")


def main():
    """Define and run a DAG with data passing (XCom)."""
    db_path = "examples.db"
    init_schema(db_path)

    with Dag("xcom_dag", db_path=db_path) as dag:
        # Task IDs default to function names: 'extract', 'transform', 'load'
        t_extract = dag.task(extract)
        t_transform = dag.task(transform)
        t_load = dag.task(load)

        # Define dependencies
        # Data is passed automatically based on argument names matching task IDs
        t_extract >> t_transform >> t_load

    print("Running XCom DAG...")
    dag.run()


if __name__ == "__main__":
    main()
