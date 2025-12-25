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
from functools import wraps
from typing import Dict, List, Optional, Any, Callable, Union, Set

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
# --- SQL Statements ---
SQL_INIT_SCHEMA = """
                  -- 1. Registered DAG definitions (prefixed)
                  CREATE TABLE IF NOT EXISTS liteflow_dags
                  (
                      dag_id      TEXT PRIMARY KEY,
                      description TEXT,
                      is_active   INTEGER DEFAULT 1,
                      created_at  INTEGER NOT NULL
                  ) STRICT;

                  -- 2. DAG execution instances (prefixed)
                  CREATE TABLE IF NOT EXISTS liteflow_dag_runs
                  (
                      run_id     TEXT PRIMARY KEY,
                      dag_id     TEXT    NOT NULL,
                      status     TEXT    NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
                      created_at INTEGER NOT NULL,
                      FOREIGN KEY (dag_id) REFERENCES liteflow_dags (dag_id)
                  ) STRICT;

                  -- 3. Individual task node states (prefixed)
                  CREATE TABLE IF NOT EXISTS liteflow_task_instances
                  (
                      run_id       TEXT    NOT NULL,
                      task_id      TEXT    NOT NULL,
                      status       TEXT    NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
                      dependencies TEXT,                 -- JSON array of task_id strings
                      timeout      INTEGER DEFAULT 3600, -- Max execution time in seconds
                      error_log    TEXT,                 -- Stack trace or error message
                      updated_at   INTEGER NOT NULL,     -- UTC Unix Timestamp
                      PRIMARY KEY (run_id, task_id),
                      FOREIGN KEY (run_id) REFERENCES liteflow_dag_runs (run_id)
                  ) STRICT;

                  -- 4. Inter-task communication (XCom) (prefixed)
                  CREATE TABLE IF NOT EXISTS liteflow_xcom
                  (
                      run_id  TEXT NOT NULL,
                      task_id TEXT NOT NULL,
                      key     TEXT NOT NULL,
                      value   BLOB, -- Pickled Python object
                      PRIMARY KEY (run_id, task_id, key),
                      FOREIGN KEY (run_id, task_id) REFERENCES liteflow_task_instances (run_id, task_id)
                  ) STRICT;

                  CREATE INDEX IF NOT EXISTS idx_liteflow_task_instances_status ON liteflow_task_instances (status); \
                  """

SQL_GET_TASK_STATES = (
    "SELECT task_id, status FROM liteflow_task_instances WHERE run_id = ?"
)

SQL_UPDATE_TASK_STATUS_ERROR = """
                               UPDATE liteflow_task_instances
                               SET status     = ?,
                                   updated_at = ?,
                                   error_log  = ?
                               WHERE run_id = ?
                                 AND task_id = ? \
                               """

SQL_UPDATE_TASK_STATUS = """
                         UPDATE liteflow_task_instances
                         SET status     = ?,
                             updated_at = ?
                         WHERE run_id = ?
                           AND task_id = ? \
                         """

SQL_UPDATE_RUN_STATUS = "UPDATE liteflow_dag_runs SET status = ? WHERE run_id = ?"

SQL_UPSERT_XCOM = """
    INSERT OR REPLACE INTO liteflow_xcom (run_id, task_id, key, value)
    VALUES (?, ?, ?, ?)
"""

SQL_GET_XCOM = """
               SELECT value
               FROM liteflow_xcom
               WHERE run_id = ?
                 AND task_id = ?
                 AND key = ? \
               """

SQL_REGISTER_DAG = """
    INSERT OR REPLACE INTO liteflow_dags (dag_id, description, created_at)
    VALUES (?, ?, ?)
"""

SQL_INSERT_DAG_RUN = "INSERT INTO liteflow_dag_runs (run_id, dag_id, status, created_at) VALUES (?, ?, ?, ?)"

SQL_INSERT_TASK_INSTANCE = """
                           INSERT INTO liteflow_task_instances (run_id, task_id, status, dependencies, timeout, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?) \
                           """


class Status:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class XComFileRef:
    path: str


def generate_uuid() -> str:
    return str(uuid.uuid4())


def _execute_task_wrapper(db_path, run_id, task_id, func, kwargs):
    """Wrapper to execute a task in a separate process."""
    logger.info(f"Worker executing task {task_id} (Run: {run_id})")
    db = LiteFlowDB(db_path)
    db.set_task_status(run_id, task_id, Status.RUNNING)
    try:
        result = func(**kwargs)
        db.store_xcom(run_id, task_id, "return_value", result)
        db.set_task_status(run_id, task_id, Status.SUCCESS)
        logger.info(f"Worker finished task {task_id} successfully")
        return None
    except Exception:
        err = traceback.format_exc()
        logger.error(f"Worker failed task {task_id}: {err}")
        db.set_task_status(run_id, task_id, Status.FAILED, error_log=err)
        return err


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
    with closing(connect(db_path)) as conn:
        with conn:
            conn.executescript(SQL_INIT_SCHEMA)


class LiteFlowDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        init_schema(self.db_path)

    def get_task_states(self, run_id: str) -> Dict[str, str]:
        """Retrieves the current status of all tasks in a run."""
        with closing(connect(self.db_path)) as conn:
            rows = conn.execute(SQL_GET_TASK_STATES, (run_id,)).fetchall()
            return {row["task_id"]: row["status"] for row in rows}

    def set_task_status(
        self, run_id: str, task_id: str, status: str, error_log: str = None
    ):
        """Updates the status and timestamp of a specific task."""
        logger.debug(f"Updating task {task_id} status to {status} (Run: {run_id})")
        with closing(connect(self.db_path)) as conn:
            with conn:
                if error_log:
                    conn.execute(
                        SQL_UPDATE_TASK_STATUS_ERROR,
                        (status, int(time.time()), error_log, run_id, task_id),
                    )
                else:
                    conn.execute(
                        SQL_UPDATE_TASK_STATUS,
                        (status, int(time.time()), run_id, task_id),
                    )

    def update_run_status(self, run_id: str, status: str):
        """Updates the overall status of a DAG run."""
        logger.info(f"Updating run {run_id} status to {status}")
        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(SQL_UPDATE_RUN_STATUS, (status, run_id))

    def store_xcom(self, run_id: str, task_id: str, key: str, value: Any):
        """Stores an XCom value, spilling to disk if too large."""
        logger.debug(f"Storing XCom for {task_id}.{key} (Run: {run_id})")
        blob = pickle.dumps(value)
        if len(blob) > XCOM_FILE_THRESHOLD:
            xcom_dir = self.db_path + "_xcom"
            os.makedirs(xcom_dir, exist_ok=True)
            filename = f"{run_id}_{task_id}_{key}.bin"
            file_path = os.path.join(xcom_dir, filename)
            with open(file_path, "wb") as f:
                f.write(blob)
            blob = pickle.dumps(XComFileRef(path=file_path))

        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(
                    SQL_UPSERT_XCOM,
                    (run_id, task_id, key, blob),
                )

    def get_xcom(self, run_id: str, task_id: str, key: str) -> Any:
        """Retrieves an XCom value, loading from disk if necessary."""
        logger.debug(f"Retrieving XCom for {task_id}.{key} (Run: {run_id})")
        with closing(connect(self.db_path)) as conn:
            row = conn.execute(
                SQL_GET_XCOM,
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


@dataclass
class Task:
    task_id: str
    func: Callable
    dag: "Dag"
    dependencies: Set[str] = field(default_factory=set)
    timeout: int = 3600

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


class Dag:
    _context: Optional["Dag"] = None

    def __init__(
        self, dag_id: str, description: str = "", db_path: str = "liteflow.db"
    ):
        self.dag_id = dag_id
        self.description = description
        self.tasks: Dict[str, Task] = {}
        self.db_path = db_path
        self.db = LiteFlowDB(db_path)

    def __enter__(self):
        Dag._context = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        Dag._context = None
        self.register()

    def register(self):
        """Registers the DAG metadata to the database."""
        logger.info(f"Registering DAG {self.dag_id}")
        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(
                    SQL_REGISTER_DAG,
                    (self.dag_id, self.description, int(time.time())),
                )

    def add_task(self, task: Task):
        """Adds a task to the DAG."""
        if task.task_id in self.tasks:
            raise ValueError(
                f"Task with id {task.task_id} already exists in DAG {self.dag_id}"
            )
        logger.debug(f"Added task {task.task_id} to DAG {self.dag_id}")
        self.tasks[task.task_id] = task

    def create_run(self) -> str:
        """Creates a new DAG run and initializes task instances."""
        run_id = generate_uuid()
        now = int(time.time())

        with closing(connect(self.db_path)) as conn:
            with conn:
                # Create DAG run
                conn.execute(
                    SQL_INSERT_DAG_RUN,
                    (run_id, self.dag_id, Status.PENDING, now),
                )

                # Snapshot task instances
                for task_id, task in self.tasks.items():
                    conn.execute(
                        SQL_INSERT_TASK_INSTANCE,
                        (
                            run_id,
                            task_id,
                            Status.PENDING,
                            json.dumps(list(task.dependencies)),
                            task.timeout,
                            now,
                        ),
                    )
        logger.info(f"Initialized DAG run {run_id} for DAG {self.dag_id}")
        return run_id

    def _resolve_task_kwargs(self, task: Task, run_id: str) -> Dict[str, Any]:
        """Resolves task arguments by fetching XCom values from dependencies."""
        sig = inspect.signature(task.func)
        kwargs = {}
        for param_name in sig.parameters:
            if param_name in task.dependencies:
                logger.debug(
                    f"Resolving dependency arg '{param_name}' for task {task.task_id}"
                )
                val = self.db.get_xcom(run_id, param_name, "return_value")
                kwargs[param_name] = val
        return kwargs

    def run(self, run_id: str = None):
        """Executes the DAG using ProcessPoolExecutor."""
        if run_id is None:
            run_id = self.create_run()

        logger.info(f"Starting DAG run {run_id} for DAG {self.dag_id}")

        # Build the graph for execution
        graph = {t_id: task.dependencies for t_id, task in self.tasks.items()}
        logger.info(f"Graph for DAG {self.dag_id}: {graph}")
        ts = graphlib.TopologicalSorter(graph)
        try:
            ts.prepare()
        except graphlib.CycleError as e:
            logger.error(f"Cycle detected in DAG {self.dag_id}: {e}")
            self.db.update_run_status(run_id, Status.FAILED)
            return run_id

        # Get current state of tasks
        task_states = self.db.get_task_states(run_id)

        # Mark run as RUNNING
        self.db.update_run_status(run_id, Status.RUNNING)

        futures = {}
        start_times = {}
        executor = concurrent.futures.ProcessPoolExecutor()
        try:
            while ts.is_active():
                ready_tasks = ts.get_ready()
                for task_id in ready_tasks:
                    current_status = task_states.get(task_id, Status.PENDING)

                    if current_status == Status.SUCCESS:
                        logger.info(f"Task {task_id} already SUCCESS, skipping.")
                        ts.done(task_id)
                        continue

                    # Execute task
                    logger.info(f"Scheduling task {task_id}")
                    task = self.tasks[task_id]

                    kwargs = self._resolve_task_kwargs(task, run_id)

                    start_times[task_id] = time.time()
                    future = executor.submit(
                        _execute_task_wrapper,
                        self.db_path,
                        run_id,
                        task_id,
                        task.func,
                        kwargs,
                    )
                    futures[future] = task_id

                if not futures:
                    if ts.is_active():
                        continue
                    break

                # Wait for at least one task to finish, or a timeout to occur
                # We use a short 1s timeout to check for our own task timeouts
                done, _ = concurrent.futures.wait(
                    futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for future in done:
                    task_id = futures.pop(future)
                    start_times.pop(task_id, None)
                    try:
                        error = future.result()
                        if error:
                            self.db.update_run_status(run_id, Status.FAILED)
                            return run_id
                        ts.done(task_id)
                    except Exception as e:
                        logger.error(f"Task {task_id} failed: {e}")
                        self.db.update_run_status(run_id, Status.FAILED)
                        return run_id

                # Check for timeouts
                now = time.time()
                for future, task_id in list(futures.items()):
                    if now - start_times[task_id] > self.tasks[task_id].timeout:
                        logger.error(
                            f"Task {task_id} timed out after {self.tasks[task_id].timeout}s"
                        )
                        self.db.set_task_status(
                            run_id, task_id, Status.FAILED, error_log="TimeoutError"
                        )
                        self.db.update_run_status(run_id, Status.FAILED)
                        # We can't easily kill the task in ProcessPoolExecutor,
                        # but we can stop the DAG.
                        return run_id
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        self.db.update_run_status(run_id, Status.SUCCESS)
        logger.info(f"DAG run {run_id} completed successfully.")
        return run_id


def task(task_id: str = None, timeout: int = 3600):
    """Decorator to define a task within a DAG."""

    def decorator(func):
        nonlocal task_id
        if task_id is None:
            task_id = func.__name__

        if Dag._context is None:
            raise RuntimeError("Tasks must be defined within a DAG context")

        t = Task(task_id=task_id, func=func, dag=Dag._context, timeout=timeout)
        Dag._context.add_task(t)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return t

    return decorator
