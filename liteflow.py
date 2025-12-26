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
from typing import Dict, List, Optional, Any, Callable, Union, Set, ClassVar, Self

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


class Status:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


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


@dataclass
class TaskInstance:
    run_id: str
    task_id: str
    status: str
    dependencies: List[str]
    timeout: int
    updated_at: int
    error_log: Optional[str] = None

    DDL: ClassVar[
        str
    ] = """
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
        CREATE INDEX IF NOT EXISTS idx_liteflow_task_instances_status ON liteflow_task_instances (status); \
        """

    @classmethod
    def create(
        cls,
        db_path: str,
        run_id: str,
        task_id: str,
        dependencies: List[str],
        timeout: int,
    ):
        """Creates a task instance record."""
        now = int(time.time())
        with closing(connect(db_path)) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO liteflow_task_instances (run_id, task_id, status, dependencies, timeout, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        task_id,
                        Status.PENDING,
                        json.dumps(dependencies),
                        timeout,
                        now,
                    ),
                )
        return cls(run_id, task_id, Status.PENDING, dependencies, timeout, now)

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


def generate_uuid() -> str:
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

    def execute_worker(self, db_path: str, ti: TaskInstance, kwargs: Dict[str, Any]):
        """Executes the task logic in the worker process."""
        logger.info(f"Worker executing task {ti.task_id} (Run: {ti.run_id})")
        ti.update_status(db_path, Status.RUNNING)
        try:
            # Execute the actual user function
            result = self.func(**kwargs)
            XCom.save(db_path, ti.run_id, ti.task_id, "return_value", result)
            ti.update_status(db_path, Status.SUCCESS)
            logger.info(f"Worker finished task {ti.task_id} successfully")
            return None
        except Exception:
            err = traceback.format_exc()
            logger.error(f"Worker failed task {ti.task_id}: {err}")
            ti.update_status(db_path, Status.FAILED, error_log=err)
            return err

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
    _context: ClassVar[Optional["Dag"]] = None

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
        VALUES (?, ?, ?)
    """

    def __enter__(self):
        Dag._context = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        Dag._context = None
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
        self, func: Callable, task_id: str = None, timeout: int = 3600, **kwargs
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
        """Creates a new DAG run entry in the database."""
        run_id = generate_uuid()
        now = int(time.time())
        with closing(connect(self.db_path)) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO liteflow_dag_runs (run_id, dag_id, status, created_at) VALUES (?, ?, ?, ?)",
                    (run_id, self.dag_id, Status.PENDING, now),
                )
        return DagRun(
            run_id=run_id, dag_id=self.dag_id, status=Status.PENDING, created_at=now
        )

    def _initialize_task_instances(self, dag_run: DagRun):
        """Creates a new DAG run and initializes task instances."""
        for task_id, task in self.tasks.items():
            TaskInstance.create(
                self.db_path,
                dag_run.run_id,
                task_id,
                list(task.dependencies),
                task.timeout,
            )

        logger.info(f"Initialized DAG run {dag_run.run_id} for DAG {self.dag_id}")
        return dag_run

    def _resolve_task_kwargs(self, task: Task, run_id: str) -> Dict[str, Any]:
        """Resolves task arguments by fetching XCom values from dependencies."""
        sig = inspect.signature(task.func)
        kwargs = task.bound_args.copy()
        for param_name in sig.parameters:
            upstream_task_id = task.arg_dependencies.get(param_name)
            if not upstream_task_id and param_name in task.dependencies:
                upstream_task_id = param_name

            if upstream_task_id:
                logger.debug(
                    f"Resolving dependency arg '{param_name}' for task {task.task_id} from {upstream_task_id}"
                )
                val = XCom.load(self.db_path, run_id, upstream_task_id, "return_value")
                kwargs[param_name] = val
        return kwargs

    def run(self) -> DagRun:
        """Executes the DAG using ProcessPoolExecutor."""
        dag_run = self.new_dag_run()
        self._initialize_task_instances(dag_run)
        logger.info(f"Starting DAG run {dag_run.run_id} for DAG {self.dag_id}")
        # Build the graph for execution
        graph = {t_id: task.dependencies for t_id, task in self.tasks.items()}
        logger.info(f"Graph for DAG {self.dag_id}: {graph}")
        ts = graphlib.TopologicalSorter(graph)
        try:
            ts.prepare()
        except graphlib.CycleError as e:
            logger.error(f"Cycle detected in DAG {self.dag_id}: {e}")
            dag_run.update_status(self.db_path, Status.FAILED)
            return dag_run
        # Get current state of tasks
        task_states = dag_run.get_all_task_states(self.db_path)
        # Mark run as RUNNING
        dag_run.update_status(self.db_path, Status.RUNNING)
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
                    kwargs = self._resolve_task_kwargs(task, dag_run.run_id)
                    start_times[task_id] = time.time()
                    ti = TaskInstance(
                        run_id=dag_run.run_id,
                        task_id=task_id,
                        status=Status.PENDING,
                        dependencies=list(task.dependencies),
                        timeout=task.timeout,
                        updated_at=int(time.time()),
                    )
                    future = executor.submit(
                        task.execute_worker,
                        self.db_path,
                        ti,
                        kwargs,
                    )
                    futures[future] = task_id
                if not futures:
                    if ts.is_active():
                        raise RuntimeError(
                            f"Deadlock detected in DAG {self.dag_id}: Tasks are pending but none are ready or running."
                        )
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
                            dag_run.update_status(self.db_path, Status.FAILED)
                            return dag_run
                        ts.done(task_id)
                    except Exception as e:
                        logger.error(f"Task {task_id} failed: {e}")
                        dag_run.update_status(self.db_path, Status.FAILED)
                        return dag_run
                # Check for timeouts
                now = time.time()
                for future, task_id in list(futures.items()):
                    if now - start_times[task_id] > self.tasks[task_id].timeout:
                        logger.error(
                            f"Task {task_id} timed out after {self.tasks[task_id].timeout}s"
                        )
                        ti = TaskInstance(
                            run_id=dag_run.run_id,
                            task_id=task_id,
                            status=Status.RUNNING,
                            dependencies=list(self.tasks[task_id].dependencies),
                            timeout=self.tasks[task_id].timeout,
                            updated_at=int(now),
                        )
                        ti.update_status(
                            self.db_path,
                            Status.FAILED,
                            error_log="TimeoutError",
                        )
                        dag_run.update_status(self.db_path, Status.FAILED)
                        # We can't easily kill the task in ProcessPoolExecutor,
                        # but we can stop the DAG.
                        return dag_run
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        dag_run.update_status(self.db_path, Status.SUCCESS)
        logger.info(f"DAG run {dag_run.run_id} completed successfully.")
        return dag_run

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
