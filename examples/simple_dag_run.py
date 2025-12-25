import sys
from pathlib import Path

python_path = Path(__file__).parent.resolve()
sys.path.insert(0, str(python_path))

from typing import Dict
from liteflow import Dag, task


def extract_data() -> Dict[str, int]:
    """Simulate data extraction."""
    return {"value": 42}


def transform_data(extract_data: Dict[str, int]) -> int:
    """Simulate data transformation using input from extract_data."""
    return extract_data["value"] * 2


def load_data(transform_data: int) -> None:
    """Simulate data loading."""
    # In a real scenario, this would write to a database or API
    return


def main() -> None:
    """Drive the simple DAG execution."""
    # The db_path will be created automatically if it doesn't exist
    with Dag("simple_etl_dag", db_path="example_flow.db") as dag:
        # Note: Task IDs must match parameter names in downstream functions
        # for automatic XCom resolution.
        t1 = task(task_id="extract_data")(extract_data)
        t2 = task(task_id="transform_data")(transform_data)
        t3 = task(task_id="load_data")(load_data)

        # Define the execution flow: extract -> transform -> load
        t1 >> t2 >> t3

    dag.run()


if __name__ == "__main__":
    main()
