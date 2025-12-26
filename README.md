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
