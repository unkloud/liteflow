# LiteFlow Development Plan

LiteFlow is a lightweight, local task orchestrator designed to provide Airflow-like capabilities (DAGs, XCom, Retries)
using only the Python standard library and SQLite. It is optimized for single-machine environments where durability and
resumption are critical.

## Core Objectives

Zero Configuration: Single SQLite file for all metadata.

Resumability: Ability to resume from the last successful task node after a failure or process restart.

Durable State: Atomic transactions for task status and data passing (XCom).

Process Isolation: Support for task timeouts using multiprocess execution.

## System Architecture

### Visual Overview

+-----------------------------------------------------------+
| USER DEFINITION (Python)                |
| @task decorators, with DAG() context, >> dependencies |
+-----------------------------+-----------------------------+
|
v
+-----------------------------------------------------------+
| ORCHESTRATION ENGINE |
| |
| 1. Registration: Map task_id -> Python function |
| 2. Materialization: Snapshots graph structure into DB |
| 3. Graph Logic: graphlib.TopologicalSorter |
| 4. Execution: ProcessPoolExecutor (Isolation/Timeout)   |
+-----------+-----------------------------------+-----------+
| |
| (Read/Write)                      | (Pickle/Unpickle)
v v
+---------------------------+ +-----------------------+
| SQLITE METADATA DB | | XCOM STORAGE |
+---------------------------+ +-----------------------+
| - dags (Metadata)         | | - task_results (BLOB) |
| - dag_runs (Execution)    | | - task_params (BLOB)  |
| - task_instances (Status) | +-----------------------+
| - static_edges (JSON)     |
+---------------------------+

### Database Schema (DDL)

The system uses INTEGER for all timestamps (UTC Unix Timestamps).

PRAGMA journal_mode=WAL;

-- 1. Registered DAG definitions
CREATE TABLE IF NOT EXISTS dags (
dag_id TEXT PRIMARY KEY,
description TEXT,
is_active INTEGER DEFAULT 1,
created_at INTEGER NOT NULL
);

-- 2. DAG execution instances
CREATE TABLE IF NOT EXISTS dag_runs (
run_id TEXT PRIMARY KEY,
dag_id TEXT NOT NULL,
status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
created_at INTEGER NOT NULL,
FOREIGN KEY (dag_id) REFERENCES dags(dag_id)
);

-- 3. Individual task node states
CREATE TABLE IF NOT EXISTS task_instances (
run_id TEXT NOT NULL,
task_id TEXT NOT NULL,
status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
dependencies TEXT, -- JSON array of task_id strings
timeout INTEGER DEFAULT 3600, -- Max execution time in seconds
error_log TEXT, -- Stack trace or error message
updated_at INTEGER NOT NULL, -- UTC Unix Timestamp
PRIMARY KEY (run_id, task_id),
FOREIGN KEY (run_id) REFERENCES dag_runs(run_id)
);

-- 4. Inter-task communication (XCom)
CREATE TABLE IF NOT EXISTS xcom (
run_id TEXT NOT NULL,
task_id TEXT NOT NULL,
key TEXT NOT NULL,
value BLOB, -- Pickled Python object
PRIMARY KEY (run_id, task_id, key),
FOREIGN KEY (run_id, task_id) REFERENCES task_instances(run_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_task_instances_status ON task_instances(status);

## Design Decisions & Rationale

| Decision                  | Rationale                                                                                |
|:--------------------------|:-----------------------------------------------------------------------------------------|
| SQLite + WAL              | ACID compliance with zero setup. WAL mode allows concurrent reads during task execution. |
| graphlib Standard Library | Standard implementation for complex dependency sorting (Python 3.9+).                    |
| Static JSON Edges         | Immutable graph structure at runtime prevents logic drift during execution.              |
| ProcessPoolExecutor       | Required for timeouts. Sub-processes can be killed; threads cannot.                      |
| Pickle for XCom           | Native support for complex Python objects (DataFrames, dicts, etc.).                     |
| Integer Timestamps        | Ensures UTC consistency and simplifies time-based queries.                               |

## Return Value Guidelines (Designer Contract)

Size: Keep results < 1MB. For larger data, return a file path string.

Type: Must be Picklable. No generators, open files, or DB connections.

Structure: Prefer dict for named access and schema flexibility.

Contract: Downstream function argument names must match upstream task_id for auto-injection.

## Implementation Roadmap

Phase 1: Foundation (Complete)

[x] Finalize Schema with Integer Timestamps and DDLs.

[x] DAG Context and Task Decorator.

[x] Static Graph Materialization logic.

Phase 2: XCom & Auto-Injection (Complete)

[x] Implement Pickle serialization.

[x] Implement parameter mapping via inspect.signature.

[x] Large object pattern (automatic file reference for >10MB).

Phase 3: Engine & Resumption (Complete)

[x] Graph reconstruction using TopologicalSorter.

[x] State Alignment (skip SUCCESS tasks).

[x] Atomic Commit-on-Success.

Phase 4: Isolation & Timeout (Complete)

[x] ProcessPoolExecutor integration.

[x] Timeout error handling and zombie task reset.
