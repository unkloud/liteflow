#!/usr/bin/env python3

import concurrent.futures
import graphlib
import inspect
import json
import logging
import os
import pickle
import sqlite3
import sys
import time
import traceback
import uuid
from contextlib import closing
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Union, Set, ClassVar

try:
    from typing import Self
except ImportError:
    Self = Any  # Fallback for Python < 3.11

SQL_PRAGMA_WAL = "PRAGMA journal_mode=WAL;"
SQL_PRAGMA_SYNCHRONOUS = "PRAGMA synchronous=NORMAL;"
MIN_SQLITE_VERSION = (3, 37, 0)
if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
    ver_str = ".".join(map(str, MIN_SQLITE_VERSION))
    raise RuntimeError(
        f"LiteFlow requires SQLite version {ver_str} or higher. Found {sqlite3.sqlite_version}."
    )

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PID:%(process)d TID:%(thread)d] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("LiteFlow")
# --- Constants ---
XCOM_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB
SCAFFOLD_TEMPLATE = """import os
import os
from liteflow import Dag, init_schema


def task_1():
    print("Hello from Task 1")


def main():
    # Use DB path from env (CLI) or default
    db_path = os.getenv("LITEFLOW_DB_PATH", "liteflow.db")
    init_schema(db_path)

    with Dag("my_new_dag", db_path=db_path, description="Auto-generated DAG") as dag:
        t1 = dag.task(
            task_1,
            task_id="task1",
        )
        t2 = dag.task(task_1, task_id="task2")
        _ = t1 >> t2
    print("Running DAG...")
    dag.run()


if __name__ == "__main__":
    main()
"""


class DeadlockError(Exception):
    pass


class Status:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    UP_FOR_RETRY = "UP_FOR_RETRY"


@dataclass
class DagRun:
    run_id: str
    dag_id: str
    status: str
    created_at: int

    DDL: ClassVar[
        str
    ] = """
        CREATE TABLE IF NOT EXISTS liteflow_dag_runs
        (
            run_id     TEXT PRIMARY KEY,
            dag_id     TEXT    NOT NULL,
            status     TEXT    NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
            created_at INTEGER NOT NULL,
            FOREIGN KEY (dag_id) REFERENCES liteflow_dags (dag_id)
        ) STRICT; \
        """

    def update_status(self, db_path: str, status: str):
        """Updates the status of this run."""
        logger.info(f"Updating run {self.run_id} status to {status}")
        with closing(connect(db_path)) as conn:
            with conn:
                conn.execute(
                    "UPDATE liteflow_dag_runs SET status = ? WHERE run_id = ?",
                    (status, self.run_id),
                )
        self.status = status

    def get_all_task_states(self, db_path: str) -> Dict[str, str]:
        """Retrieves status map for all tasks in this run."""
        with closing(connect(db_path)) as conn:
            rows = conn.execute(
                "SELECT task_id, status FROM liteflow_task_instances WHERE run_id = ?",
                (self.run_id,),
            ).fetchall()
            return {row["task_id"]: row["status"] for row in rows}

    def maintain_task_instances(
        self,
        db_path: str,
        tasks_data: List[Dict[str, Any]],
    ) -> Dict[str, "TaskInstance"]:
        """Prepares multiple task instances for execution, updating DB if they are retries."""
        now = int(time.time())
        retries_params = []
        instances = {}

        for data in tasks_data:
            task_id = data["task_id"]
            dependencies = data["dependencies"]
            timeout = data["timeout"]
            try_number = data["try_number"]

            if try_number > 1:
                retries_params.append(
                    (
                        self.run_id,
                        task_id,
                        Status.PENDING,
                        json.dumps(dependencies),
                        timeout,
                        try_number,
                        now,
                    )
                )
            instances[task_id] = TaskInstance(
                self.run_id,
                task_id,
                Status.PENDING,
                dependencies,
                timeout,
                try_number,
                now,
            )

        if retries_params:
            with closing(connect(db_path)) as conn:
                with conn:
                    conn.executemany(
                        "INSERT OR REPLACE INTO liteflow_task_instances (run_id, task_id, status, dependencies, timeout, try_number, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        retries_params,
                    )
        return instances

    def get_task_instance(self, db_path: str, task_id: str) -> Optional["TaskInstance"]:
        """Retrieves a task instance from the database."""
        with closing(connect(db_path)) as conn:
            row = conn.execute(
                "SELECT status, dependencies, timeout, try_number, updated_at, error_log FROM liteflow_task_instances WHERE run_id = ? AND task_id = ?",
                (self.run_id, task_id),
            ).fetchone()
            if row:
                return TaskInstance(
                    run_id=self.run_id,
                    task_id=task_id,
                    status=row["status"],
                    dependencies=(
                        json.loads(row["dependencies"]) if row["dependencies"] else []
                    ),
                    timeout=row["timeout"],
                    try_number=row["try_number"],
                    updated_at=row["updated_at"],
                    error_log=row["error_log"],
                )
        return None

    def delete(self, db_path: str):
        """Deletes this DAG run and all associated tasks and XComs."""
        logger.info(f"Deleting DAG run {self.run_id}")

        # 1. Clean up XCom files on disk
        xcom_dir = db_path + "_xcom"
        if os.path.exists(xcom_dir):
            for filename in os.listdir(xcom_dir):
                # Filename format: {run_id}_{task_id}_{key}.bin
                if filename.startswith(f"{self.run_id}_"):
                    try:
                        os.remove(os.path.join(xcom_dir, filename))
                    except OSError as e:
                        logger.warning(f"Failed to delete XCom file {filename}: {e}")

        # 2. Database cleanup
        with closing(connect(db_path)) as conn:
            with conn:
                conn.execute(
                    "DELETE FROM liteflow_xcom WHERE run_id = ?", (self.run_id,)
                )
                conn.execute(
                    "DELETE FROM liteflow_task_instances WHERE run_id = ?",
                    (self.run_id,),
                )
                conn.execute(
                    "DELETE FROM liteflow_dag_runs WHERE run_id = ?", (self.run_id,)
                )


@dataclass
class TaskInstance:
    run_id: str
    task_id: str
    status: str
    dependencies: List[str]
    timeout: int
    try_number: int
    updated_at: int
    error_log: Optional[str] = None

    DDL: ClassVar[
        str
    ] = """
        CREATE TABLE IF NOT EXISTS liteflow_task_instances
        (
            run_id       TEXT    NOT NULL,
            task_id      TEXT    NOT NULL,
            status       TEXT    NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'UP_FOR_RETRY')),
            dependencies TEXT,                 -- JSON array of task_id strings
            timeout      INTEGER DEFAULT 3600, -- Max execution time in seconds
            try_number   INTEGER DEFAULT 1,    -- Attempt number
            error_log    TEXT,                 -- Stack trace or error message
            updated_at   INTEGER NOT NULL,     -- UTC Unix Timestamp
            PRIMARY KEY (run_id, task_id),
            FOREIGN KEY (run_id) REFERENCES liteflow_dag_runs (run_id)
        ) STRICT;
        CREATE INDEX IF NOT EXISTS idx_liteflow_task_instances_status ON liteflow_task_instances (status); \
        """

    def update_status(self, db_path: str, status: str, error_log: str = None):
        """Updates the status of this task instance."""
        logger.debug(
            f"Updating task {self.task_id} status to {status} (Run: {self.run_id})"
        )
        now = int(time.time())
        with closing(connect(db_path)) as conn:
            with conn:
                if error_log:
                    conn.execute(
                        "UPDATE liteflow_task_instances SET status = ?, updated_at = ?, error_log = ? WHERE run_id = ? AND task_id = ?",
                        (status, now, error_log, self.run_id, self.task_id),
                    )
                else:
                    conn.execute(
                        "UPDATE liteflow_task_instances SET status = ?, updated_at = ? WHERE run_id = ? AND task_id = ?",
                        (status, now, self.run_id, self.task_id),
                    )
        self.status = status
        self.updated_at = now
        if error_log:
            self.error_log = error_log

    def xcom_push(self, db_path: str, key: str, value: Any):
        """Stores an XCom value for this task instance."""
        XCom.save(db_path, self.run_id, self.task_id, key, value)

    def xcom_pull(self, db_path: str, key: str) -> Any:
        """Retrieves an XCom value from a specific task in the same run."""
        return XCom.load(db_path, self.run_id, self.task_id, key)

    @classmethod
    def load(cls, db_path: str, run_id: str, task_id: str) -> Optional[Self]:
        """Retrieves a task instance from the database."""
        with closing(connect(db_path)) as conn:
            row = conn.execute(
                "SELECT status, dependencies, timeout, try_number, updated_at, error_log FROM liteflow_task_instances WHERE run_id = ? AND task_id = ?",
                (run_id, task_id),
            ).fetchone()
            if row:
                return cls(
                    run_id=run_id,
                    task_id=task_id,
                    status=row["status"],
                    dependencies=(
                        json.loads(row["dependencies"]) if row["dependencies"] else []
                    ),
                    timeout=row["timeout"],
                    try_number=row["try_number"],
                    updated_at=row["updated_at"],
                    error_log=row["error_log"],
                )
        return None


@dataclass
class XComFileRef:
    path: str


@dataclass
class XCom:
    run_id: str
    task_id: str
    key: str
    value: Any

    DDL: ClassVar[
        str
    ] = """
        CREATE TABLE IF NOT EXISTS liteflow_xcom
        (
            run_id  TEXT NOT NULL,
            task_id TEXT NOT NULL,
            key     TEXT NOT NULL,
            value   BLOB, -- Pickled Python object
            PRIMARY KEY (run_id, task_id, key),
            FOREIGN KEY (run_id, task_id) REFERENCES liteflow_task_instances (run_id, task_id)
        ) STRICT; \
        """

    @classmethod
    def save(cls, db_path: str, run_id: str, task_id: str, key: str, value: Any):
        """Stores an XCom value, spilling to disk if too large."""
        logger.debug(f"Storing XCom for {task_id}.{key} (Run: {run_id})")
        blob = pickle.dumps(value)
        if len(blob) > XCOM_FILE_THRESHOLD:
            xcom_dir = db_path + "_xcom"
            os.makedirs(xcom_dir, exist_ok=True)
            filename = f"{run_id}_{task_id}_{key}.bin"
            file_path = os.path.join(xcom_dir, filename)
            with open(file_path, "wb") as f:
                f.write(blob)
            blob = pickle.dumps(XComFileRef(path=file_path))

        with closing(connect(db_path)) as conn:
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO liteflow_xcom (run_id, task_id, key, value) VALUES (?, ?, ?, ?)",
                    (run_id, task_id, key, blob),
                )

    @classmethod
    def load(cls, db_path: str, run_id: str, task_id: str, key: str) -> Any:
        """Retrieves an XCom value, loading from disk if necessary."""
        logger.debug(f"Retrieving XCom for {task_id}.{key} (Run: {run_id})")
        with closing(connect(db_path)) as conn:
            row = conn.execute(
                "SELECT value FROM liteflow_xcom WHERE run_id = ? AND task_id = ? AND key = ?",
                (run_id, task_id, key),
            ).fetchone()
            if row:
                val = pickle.loads(row["value"])
                if isinstance(val, XComFileRef):
                    if not os.path.exists(val.path):
                        raise FileNotFoundError(f"XCom file missing: {val.path}")
                    with open(val.path, "rb") as f:
                        return pickle.loads(f.read())
                return val
            return None


def new_uuid() -> str:
    return str(uuid.uuid4())


def connect(db_path: str) -> sqlite3.Connection:
    """Creates and configures a SQLite connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(SQL_PRAGMA_SYNCHRONOUS)
    conn.execute(SQL_PRAGMA_WAL)
    if sys.version_info >= (3, 12):
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
        conn.setconfig(sqlite3.SQLITE_DBCONFIG_ENABLE_FKEY, True)
    return conn


def init_schema(db_path: str):
    """Initializes the database schema with necessary tables."""
    logger.debug(f"Initializing DB schema at {db_path}")
    # Create tables in topological order
    entities = [Dag, DagRun, TaskInstance, XCom]
    with closing(connect(db_path)) as conn:
        for entity in entities:
            logger.info(f"Creating {entity.__name__}")
            conn.executescript(entity.DDL)


@dataclass
class Task:
    task_id: str
    func: Callable
    dag: Optional["Dag"] = None
    dependencies: Set[str] = field(default_factory=set)
    arg_dependencies: Dict[str, str] = field(default_factory=dict)
    bound_args: Dict[str, Any] = field(default_factory=dict)
    timeout: int = 3600
    retries: int = 0
    retry_delay: int = 0

    def execute_worker(self, db_path: str, ti: TaskInstance, kwargs: Dict[str, Any]):
        """Executes the task logic in the worker process."""
        logger.info(f"Worker executing task {ti.task_id} (Run: {ti.run_id})")
        ti.update_status(db_path, Status.RUNNING)
        try:
            # Execute the actual user function
            result = self.func(**kwargs)
            ti.xcom_push(db_path, "return_value", result)
            ti.update_status(db_path, Status.SUCCESS)
            logger.info(f"Worker finished task {ti.task_id} successfully")
            return None
        except Exception:
            err = traceback.format_exc()
            logger.error(f"Worker failed task {ti.task_id}: {err}")
            ti.update_status(db_path, Status.FAILED, error_log=err)
            return err

    def resolve_kwargs(self, db_path: str, ti: TaskInstance) -> Dict[str, Any]:
        """Resolves task arguments by fetching XCom values from dependencies."""
        sig = inspect.signature(self.func)
        kwargs = self.bound_args.copy()
        for param_name in sig.parameters:
            upstream_task_id = self.arg_dependencies.get(param_name)
            if not upstream_task_id and param_name in self.dependencies:
                upstream_task_id = param_name
            if upstream_task_id:
                logger.debug(
                    f"Resolving dependency arg '{param_name}' for task {self.task_id} from {upstream_task_id}"
                )
                upstream_ti = TaskInstance.load(db_path, ti.run_id, upstream_task_id)
                val = upstream_ti.xcom_pull(db_path, "return_value")
                kwargs[param_name] = val
        return kwargs

    def __rshift__(self, other: Union["Task", List["Task"]]):
        """Allows use of >> operator to set dependencies."""
        if isinstance(other, list):
            for t in other:
                t.dependencies.add(self.task_id)
        else:
            other.dependencies.add(self.task_id)
        return other

    def __repr__(self):
        return f"<Task {self.task_id}>"

    def __getstate__(self):
        # Prevent pickling the DAG object to the worker
        state = self.__dict__.copy()
        state["dag"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


@dataclass
class Dag:
    dag_id: str
    db_path: str
    description: str = ""
    tasks: Dict[str, Task] = field(default_factory=dict, init=False)

    DDL: ClassVar[
        str
    ] = """
        CREATE TABLE IF NOT EXISTS liteflow_dags
        (
            dag_id      TEXT PRIMARY KEY,
            description TEXT,
            is_active   INTEGER DEFAULT 1,
            created_at  INTEGER NOT NULL
        ) STRICT;
        CREATE INDEX IF NOT EXISTS idx_liteflow_dags_dag_id ON liteflow_dags (dag_id); \
        """

    SQL_PERSIST_DAG: ClassVar[
        str
    ] = """
        INSERT OR REPLACE INTO liteflow_dags (dag_id, description, created_at)
        VALUES (?, ?, ?) \
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.persist()

    def persist(self):
        """Persists the DAG metadata to the database."""
        logger.info(f"Persisting DAG {self.dag_id}")
        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(
                    self.SQL_PERSIST_DAG,
                    (self.dag_id, self.description, int(time.time())),
                )

    def task(
        self,
        func: Callable,
        task_id: str = None,
        timeout: int = 3600,
        retries: int = 0,
        retry_delay: int = 0,
        **kwargs,
    ) -> Task:
        """Creates a Task from a function and adds it to the DAG."""
        if task_id is None:
            task_id = func.__name__

        deps = set()
        arg_deps = {}
        bound_args = {}

        for param_name, val in kwargs.items():
            if isinstance(val, Task):
                deps.add(val.task_id)
                arg_deps[param_name] = val.task_id
            else:
                bound_args[param_name] = val

        t = Task(
            task_id=task_id,
            func=func,
            dag=self,
            timeout=timeout,
            dependencies=deps,
            retries=retries,
            retry_delay=retry_delay,
            arg_dependencies=arg_deps,
            bound_args=bound_args,
        )
        return self.add_task(t)

    def add_task(self, task: Task) -> Task:
        """Adds a task to the DAG."""
        if task.task_id in self.tasks:
            raise ValueError(
                f"Task with id {task.task_id} already exists in DAG {self.dag_id}"
            )
        logger.debug(f"Added task {task.task_id} to DAG {self.dag_id}")
        task.dag = self
        self.tasks[task.task_id] = task
        return task

    def new_dag_run(self) -> DagRun:
        """Creates a new DAG run and initializes task instances in a single transaction."""
        run_id = new_uuid()
        now = int(time.time())
        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO liteflow_dag_runs (run_id, dag_id, status, created_at) VALUES (?, ?, ?, ?)",
                    (run_id, self.dag_id, Status.PENDING, now),
                )
                task_params = [
                    (
                        run_id,
                        task_id,
                        Status.PENDING,
                        json.dumps(list(task.dependencies)),
                        task.timeout,
                        1,
                        now,
                    )
                    for task_id, task in self.tasks.items()
                ]
                conn.executemany(
                    "INSERT OR REPLACE INTO liteflow_task_instances (run_id, task_id, status, dependencies, timeout, try_number, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    task_params,
                )
        logger.info(f"Initialized DAG run {run_id} for DAG {self.dag_id}")
        return DagRun(
            run_id=run_id, dag_id=self.dag_id, status=Status.PENDING, created_at=now
        )

    def run(self) -> DagRun:
        """Executes the DAG using ProcessPoolExecutor."""
        # Build the graph for execution
        graph = {t_id: task.dependencies for t_id, task in self.tasks.items()}
        logger.info(f"Graph for DAG {self.dag_id}: {graph}")
        ts = graphlib.TopologicalSorter(graph)
        try:
            ts.prepare()
        except graphlib.CycleError as e:
            logger.error(f"Cycle detected in DAG {self.dag_id}: {e}")
            raise

        dag_run = self.new_dag_run()
        logger.info(f"Starting DAG run {dag_run.run_id} for DAG {self.dag_id}")

        futures = {}
        start_times = {}
        waiting_for_retry = {}  # task_id -> wake_up_timestamp
        task_retry_counts = {}  # task_id -> current_try_number

        executor = concurrent.futures.ProcessPoolExecutor()
        try:
            # Mark run as RUNNING
            dag_run.update_status(self.db_path, Status.RUNNING)
            while ts.is_active() or futures or waiting_for_retry:
                # 1. Check for retries ready to run
                now = time.time()
                ready_retries = []
                for tid, wake_time in list(waiting_for_retry.items()):
                    if now >= wake_time:
                        del waiting_for_retry[tid]
                        ready_retries.append(tid)

                # 2. Get new ready tasks from graph
                new_ready_tasks = ts.get_ready() if ts.is_active() else []

                # 3. Schedule tasks
                tasks_to_schedule = list(new_ready_tasks) + ready_retries
                if tasks_to_schedule:
                    batch_data = []
                    for task_id in tasks_to_schedule:
                        task = self.tasks[task_id]
                        current_try = task_retry_counts.get(task_id, 0) + 1
                        task_retry_counts[task_id] = current_try
                        batch_data.append(
                            {
                                "task_id": task_id,
                                "dependencies": list(task.dependencies),
                                "timeout": task.timeout,
                                "try_number": current_try,
                            }
                        )

                    tis_map = dag_run.maintain_task_instances(self.db_path, batch_data)

                    for task_id in tasks_to_schedule:
                        logger.info(f"Scheduling task {task_id}")
                        task = self.tasks[task_id]
                        ti = tis_map[task_id]
                        kwargs = task.resolve_kwargs(self.db_path, ti)
                        start_times[task_id] = time.time()
                        future = executor.submit(
                            task.execute_worker,
                            self.db_path,
                            ti,
                            kwargs,
                        )
                        futures[future] = task_id

                if not futures and not waiting_for_retry:
                    if ts.is_active():
                        raise DeadlockError(
                            f"Deadlock detected in DAG {self.dag_id}: Tasks are pending but none are ready or running."
                        )
                    break

                # Wait for at least one task to finish, or a timeout to occur
                wait_timeout = 1.0
                if waiting_for_retry:
                    next_wake = min(waiting_for_retry.values())
                    wait_timeout = max(0.1, next_wake - time.time())
                    # Cap at 1s to ensure we check for timeouts regularly
                    wait_timeout = min(wait_timeout, 1.0)

                done, _ = concurrent.futures.wait(
                    futures,
                    timeout=wait_timeout,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    task_id = futures.pop(future)
                    start_times.pop(task_id, None)
                    try:
                        error = future.result()
                        if error:
                            # Task failed with exception
                            task = self.tasks[task_id]
                            current_try = task_retry_counts.get(task_id, 1)
                            if current_try <= task.retries:
                                logger.warning(
                                    f"Task {task_id} failed. Retrying in {task.retry_delay}s (Attempt {current_try}/{task.retries + 1})"
                                )
                                ti = dag_run.get_task_instance(self.db_path, task_id)
                                ti.update_status(
                                    self.db_path, Status.UP_FOR_RETRY, error_log=error
                                )
                                waiting_for_retry[task_id] = (
                                    time.time() + task.retry_delay
                                )
                            else:
                                logger.error(
                                    f"Task {task_id} failed and exhausted retries."
                                )
                                dag_run.update_status(self.db_path, Status.FAILED)
                                return dag_run
                        else:
                            ts.done(task_id)
                    except Exception as e:
                        logger.error(f"Task {task_id} failed: {e}")
                        # FIX: Update the specific task to FAILED so it doesn't stay RUNNING
                        ti = dag_run.get_task_instance(self.db_path, task_id)
                        if ti:
                            ti.update_status(
                                self.db_path, Status.FAILED, error_log=str(e)
                            )
                        dag_run.update_status(self.db_path, Status.FAILED)
                        return dag_run

                # Check for timeouts
                now = time.time()
                for future, task_id in list(futures.items()):
                    if now - start_times[task_id] > self.tasks[task_id].timeout:
                        logger.error(
                            f"Task {task_id} timed out after {self.tasks[task_id].timeout}s"
                        )
                        # Stop tracking this future (fire and forget zombie process)
                        del futures[future]
                        start_times.pop(task_id, None)

                        task = self.tasks[task_id]
                        current_try = task_retry_counts.get(task_id, 1)

                        ti = dag_run.get_task_instance(self.db_path, task_id)

                        if current_try <= task.retries:
                            logger.warning(
                                f"Task {task_id} timed out. Retrying in {task.retry_delay}s (Attempt {current_try}/{task.retries + 1})"
                            )
                            ti.update_status(
                                self.db_path,
                                Status.UP_FOR_RETRY,
                                error_log="TimeoutError",
                            )
                            waiting_for_retry[task_id] = time.time() + task.retry_delay
                        else:
                            ti.update_status(
                                self.db_path, Status.FAILED, error_log="TimeoutError"
                            )
                            dag_run.update_status(self.db_path, Status.FAILED)
                            return dag_run
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        dag_run.update_status(self.db_path, Status.SUCCESS)
        logger.info(f"DAG run {dag_run.run_id} completed successfully.")
        return dag_run

    @classmethod
    def delete_dag(cls, db_path: str, dag_id: str):
        """Deletes a DAG and all associated runs, tasks, and XComs."""
        logger.info(f"Deleting DAG {dag_id} from {db_path}")

        # 1. Identify runs to clean up external XCom files
        with closing(connect(db_path)) as conn:
            rows = conn.execute(
                "SELECT run_id FROM liteflow_dag_runs WHERE dag_id = ?", (dag_id,)
            ).fetchall()
            run_ids = {row["run_id"] for row in rows}

        # 2. Clean up XCom files on disk
        xcom_dir = db_path + "_xcom"
        if os.path.exists(xcom_dir) and run_ids:
            for filename in os.listdir(xcom_dir):
                # Filename format: {run_id}_{task_id}_{key}.bin
                parts = filename.split("_", 1)
                if parts and parts[0] in run_ids:
                    try:
                        os.remove(os.path.join(xcom_dir, filename))
                    except OSError as e:
                        logger.warning(f"Failed to delete XCom file {filename}: {e}")

        # 3. Database cleanup (Child tables first)
        with closing(connect(db_path)) as conn:
            with conn:
                # Delete XCom entries
                conn.execute(
                    "DELETE FROM liteflow_xcom WHERE run_id IN (SELECT run_id FROM liteflow_dag_runs WHERE dag_id = ?)",
                    (dag_id,),
                )
                # Delete TaskInstances
                conn.execute(
                    "DELETE FROM liteflow_task_instances WHERE run_id IN (SELECT run_id FROM liteflow_dag_runs WHERE dag_id = ?)",
                    (dag_id,),
                )
                # Delete DagRuns
                conn.execute(
                    "DELETE FROM liteflow_dag_runs WHERE dag_id = ?", (dag_id,)
                )
                # Delete Dag
                conn.execute("DELETE FROM liteflow_dags WHERE dag_id = ?", (dag_id,))

    @classmethod
    def load(cls, db_path: str, dag_id: str) -> Self:
        """Loads a DAG from the database."""
        with closing(connect(db_path)) as conn:
            row = conn.execute(
                "SELECT dag_id, description FROM liteflow_dags WHERE dag_id = ?",
                (dag_id,),
            ).fetchone()
            if row:
                # Note: This only loads the DAG metadata. Tasks are not stored in the DB
                # as they are defined in code. The DAG object is primarily for orchestrating runs.
                return cls(
                    dag_id=row["dag_id"],
                    db_path=db_path,
                    description=row["description"],
                )
            raise ValueError(f"DAG with ID '{dag_id}' not found in database.")

    @classmethod
    def all_dags(cls, db_path: str) -> List[Self]:
        """Lists all DAGs in the database."""
        with closing(connect(db_path)) as conn:
            rows = conn.execute(
                "SELECT dag_id, description FROM liteflow_dags ORDER BY dag_id"
            ).fetchall()
            return [
                cls(
                    dag_id=row["dag_id"],
                    db_path=db_path,
                    description=row["description"],
                )
                for row in rows
            ]


def visualize(db_path: str, dag_id_or_run_id: str):
    """Generates a Mermaid diagram for a DAG or Run ID and opens it in the browser."""
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        sys.exit(1)

    import tempfile
    import webbrowser

    run_id = dag_id_or_run_id
    # Try to load as DAG to see if it exists
    try:
        dag = Dag.load(db_path, dag_id_or_run_id)
        # DAG exists, find the latest run to visualize structure
        with closing(connect(db_path)) as conn:
            row = conn.execute(
                "SELECT run_id FROM liteflow_dag_runs WHERE dag_id = ? ORDER BY created_at DESC LIMIT 1",
                (dag.dag_id,),
            ).fetchone()
        if row:
            run_id = row["run_id"]
            print(f"Visualizing latest run '{run_id}' for DAG '{dag.dag_id}'...")
        else:
            print(
                f"DAG '{dag.dag_id}' has no runs. Cannot visualize structure without task definitions.",
                file=sys.stderr,
            )
            return
    except ValueError:
        # Not a DAG ID, assume it is a Run ID
        pass
    with closing(connect(db_path)) as conn:
        mermaid_lines = [
            "graph LR",
            "  %% Styles",
            "  classDef SUCCESS fill:#d4edda,stroke:#155724,stroke-width:2px,color:#155724;",
            "  classDef FAILED fill:#f8d7da,stroke:#721c24,stroke-width:2px,color:#721c24;",
            "  classDef RUNNING fill:#cce5ff,stroke:#004085,stroke-width:2px,color:#004085;",
            "  classDef PENDING fill:#e2e3e5,stroke:#383d41,stroke-width:2px,color:#383d41;",
            "  classDef UP_FOR_RETRY fill:#fff3cd,stroke:#856404,stroke-width:2px,color:#856404;",
            "  %% Nodes & Edges",
        ]
        rows = conn.execute(
            "SELECT task_id, status, dependencies FROM liteflow_task_instances WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        if not rows:
            print(f"Warning: No tasks found for run_id: {run_id}", file=sys.stderr)
        for row in rows:
            t_id = row["task_id"]
            status = row["status"]
            deps = json.loads(row["dependencies"]) if row["dependencies"] else []
            mermaid_lines.append(f"  {t_id}[{t_id}]:::{status}")
            for dep in deps:
                mermaid_lines.append(f"  {dep} --> {t_id}")
        mermaid_graph = "\n".join(mermaid_lines)
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LiteFlow Visualization: {run_id}</title>
    <!-- Import Mermaid library from CDN -->
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{ startOnLoad: true }});
    </script>
    <meta charset="UTF-8">
</head>
<body>
    <div class="mermaid">
{mermaid_graph}
    </div>
</body>
</html>"""
        with tempfile.NamedTemporaryFile(
            delete=False,
            prefix=f"liteflow_{dag_id_or_run_id}_",
            suffix=".html",
            mode="w",
        ) as tmp:
            tmp.write(html_content)
            url = "file://" + os.path.abspath(tmp.name)
            print(f"Opening visualization in browser: {url}")
            webbrowser.open(url)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LiteFlow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    # List
    list_parser = subparsers.add_parser("list", help="List all DAGs")
    list_parser.add_argument("--db", required=True, help="Path to SQLite database")
    # Delete
    del_parser = subparsers.add_parser("delete", help="Delete a DAG")
    del_parser.add_argument("dag_id", help="ID of the DAG to delete")
    del_parser.add_argument("--db", required=True, help="Path to SQLite database")
    # Visualize
    viz_parser = subparsers.add_parser(
        "visualize", help="Generate Mermaid diagram for a run"
    )
    viz_parser.add_argument("identifier", help="DAG ID or Run ID to visualize")
    viz_parser.add_argument("--db", required=True, help="Path to SQLite database")
    # Scaffold
    scaffold_parser = subparsers.add_parser("scaffold", help="Generate a DAG template")
    args = parser.parse_args()
    if args.command == "list":
        if not os.path.exists(args.db):
            print(f"Database not found at {args.db}")
            sys.exit(1)
        dags = Dag.all_dags(args.db)
        if not dags:
            print("No DAGs found.")
        else:
            print(f"{'DAG ID':<30} | {'Description'}")
            print("-" * 80)
            for dag in dags:
                print(f"{dag.dag_id:<30} | {dag.description}")
    elif args.command == "delete":
        if not os.path.exists(args.db):
            print(f"Database not found at {args.db}")
            sys.exit(1)
        Dag.delete_dag(args.db, args.dag_id)
        print(f"Deleted DAG '{args.dag_id}' and associated data.")
    elif args.command == "visualize":
        visualize(args.db, args.identifier)
    elif args.command == "scaffold":
        print(SCAFFOLD_TEMPLATE)
