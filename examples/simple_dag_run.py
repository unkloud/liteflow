import sys
from pathlib import Path

python_path = Path(__file__).parent.resolve()
sys.path.insert(0, str(python_path))

from typing import Dict
from liteflow import Dag, init_schema


def extract_data() -> Dict[str, int]:
    """Simulate data extraction."""
    return {"value": 42}


def transform_data(extract_data: Dict[str, int]) -> int:
    """Simulate data transformation using input from extract_data."""
    return extract_data["value"] * 2


def load_data(transform_data: int) -> str:
    """Simulate data loading."""
    # In a real scenario, this would write to a database or API
    return "Data loaded successfully."


def main():
    """Drive the simple DAG execution."""
    # The db_path will be created automatically if it doesn't exist
    init_schema("example_flow.db")
    with Dag("simple_etl_dag", db_path="example_flow.db") as dag:
        # Create tasks from functions
        t_extract = dag.task(extract_data)
        t_transform = dag.task(transform_data)
        t_load = dag.task(load_data)

        # Define dependencies
        t_extract >> t_transform >> t_load
    dag.run()


if __name__ == "__main__":
    main()
