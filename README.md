# liteflow

## Disclaimer

- Created by an AI agent.
- Evolving; use with caution.
- For dog-fooding only; no intent to serve change requests.

## What is liteflow?

Liteflow is a lightweight, embedded workflow orchestration library for Python. It allows you to define Directed Acyclic
Graphs (DAGs) of tasks using standard Python code, managing execution order, retries, and data passing (XComs) backed by
a local SQLite database.

Think of it as "Airflow-lite" — it provides the core orchestration logic (dependencies, state management, parallelism)
without the operational overhead of running a web server, scheduler service, or message broker.

## Why liteflow?

While tools like Airflow, Prefect, and Dagster are powerful, they often require significant infrastructure setup (
Postgres, Redis, separate worker processes, web UIs).

**Liteflow is designed for:**

- **Zero Infrastructure:** Just a Python script and a SQLite file.
- **Instant Start:** No `docker-compose` or `helm charts` needed.
- **Portability:** The entire state is in a single `.db` file.
- **Local Parallelism:** Uses Python's `ProcessPoolExecutor` to run tasks concurrently on a single machine.

## Possible User Scenarios

1. **Local Data Pipelines:** ETL jobs running on a researcher's laptop or a single VPS.
2. **Embedded Orchestration:** Adding workflow management inside a larger Python application without adding external
   dependencies.
3. **Dev/Test Environments:** Prototyping DAG logic locally before migrating to enterprise orchestrators.
4. **Home Lab Automation:** Managing personal scripts and backups where a full Airflow instance is overkill.

## What it is NOT

- **Not a Distributed System:** Liteflow runs on a single node. It does not distribute tasks across a cluster of
  workers.
- **Not High Availability:** It does not have failover mechanisms for the scheduler itself.
- **Not a Web UI:** Interaction is primarily through code and CLI.
- **Not for Big Data Scale:** While it handles heavy tasks, the orchestration throughput is limited by SQLite and the
  single machine's resources.

## Design Constraints

1. **Code as Definition**
   The database stores execution state (history, logs, XComs) and DAG metadata, but it does NOT
   store the task code or graph structure. The Python script is the source of truth for the DAG
   definition.

2. **Dag.load() Behavior**
   Because code is not stored in the DB, `Dag.load()` retrieves only metadata (ID, description).
   It returns a `Dag` object that is useful for inspection or history queries, but it cannot be
   used to `run()` the workflow because it lacks the task definitions. To execute a DAG, you must
   run the Python script where the tasks are defined.

3. **Visualization Limitations**
   The `visualize` command generates diagrams based on *historical execution data* (TaskInstances)
   stored in the database. It cannot visualize a DAG structure purely from the DAG ID unless a
   run has previously been initialized or executed. Consequently, visualization is an inspection
   tool for deployed/executed workflows, not a development tool for designing new graphs.

## Getting Started

### 1. Scaffold a new DAG

Liteflow includes a CLI to generate a template.

```bash
# Generate a boilerplate DAG file
python3 -m liteflow scaffold > my_workflow.py
```

### 2. Run the Workflow

Execute the Python script directly. The DAG will initialize the SQLite database (if missing) and execute the tasks.

```bash
python3 my_workflow.py
```

### 3. Visualize the Run

You can visualize the execution status of a DAG run using the `visualize` command. This generates a Mermaid diagram
showing task dependencies and their status.
Using `examples/complex_workflow.py` as an example:

1. Run the workflow:
2. Visualize the latest run:

```bash
   # Visualize the latest run for DAG ID 'complex_workflow'
   python3 -m liteflow visualize layered_dag_171 --db liteflow.db
```     
