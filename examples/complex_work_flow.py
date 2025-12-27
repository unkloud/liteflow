import random
import sys
import time
from pathlib import Path

# Add project root to path to import liteflow
sys.path.insert(0, str(Path(__file__).parents[1]))
from liteflow import Dag, init_schema


def generate(idx):
    """Layer 1: Simulates data generation."""
    # Simulate slight variance in IO
    time.sleep(random.uniform(0.001, 0.01))
    return idx


def process(val, factor):
    """Layer 2: Simulates processing data from Layer 1."""
    time.sleep(random.uniform(0.001, 0.01))
    return val * factor


def combine(a, b):
    """Layer 3: Combines two results from Layer 2."""
    return a + b


def finalize():
    """Layer 4: Final step that runs after everything else."""
    return "Workflow Complete"


def main():
    """Define and run a complex layered DAG with ~170 nodes."""
    db_path = "examples.db"
    init_schema(db_path)
    print("Constructing DAG...")
    with Dag(
        "layered_dag_171", db_path=db_path, description="Complex Layered DAG"
    ) as dag:

        # Layer 1: 20 Generators
        layer1 = []
        for i in range(20):
            t = dag.task(generate, task_id=f"gen_{i}", idx=i)
            layer1.append(t)

        # Layer 2: 100 Processors
        # Each depends on one random node from Layer 1 (Data passing)
        layer2 = []
        for i in range(100):
            parent = random.choice(layer1)
            t = dag.task(process, task_id=f"proc_{i}", val=parent, factor=i)
            layer2.append(t)

        # Layer 3: 50 Combiners
        # Each depends on two random nodes from Layer 2 (Data passing)
        layer3 = []
        for i in range(50):
            p1 = random.choice(layer2)
            p2 = random.choice(layer2)
            t = dag.task(combine, task_id=f"comb_{i}", a=p1, b=p2)
            layer3.append(t)

        # Layer 4: 1 Final Node
        # Depends on all nodes in Layer 3 (Control flow dependency only)
        final = dag.task(finalize, task_id="final")

        # Apply dependencies: All Layer 3 tasks must finish before Final
        for t in layer3:
            t >> final

    print(f"DAG '{dag.dag_id}' created with {len(dag.tasks)} tasks.")
    print("Starting execution (this may take a moment)...")
    start = time.time()
    dag.run()
    end = time.time()
    print(f"Execution finished in {end - start:.2f} seconds.")


if __name__ == "__main__":
    main()
