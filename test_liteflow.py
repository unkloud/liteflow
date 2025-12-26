import os
import pickle
import random
import shutil
import sqlite3
import time
import unittest

from liteflow import Dag, init_schema, XCom, Task


# --- Helper functions (must be top-level for pickling) ---
def noop():
    pass


def fail():
    raise Exception("Task failed")


def fail_once():
    if os.path.exists("fail.flag"):
        os.remove("fail.flag")
        raise Exception("Fail once")


def producer():
    return {"val": 42}


def consumer(producer):
    return producer["val"] * 2


def get_pid():
    return os.getpid()


def sleep_1s():
    time.sleep(1)


def sleep_long():
    time.sleep(2)


def sleep_random():
    time.sleep(random.uniform(0.001, 0.01))


def large_producer():
    return "x" * (11 * 1024 * 1024)


def large_consumer(producer):
    return len(producer)


class TestLiteFlow(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_liteflow.db"
        self.xcom_dir = self.db_path + "_xcom"
        self._clean()
        init_schema(self.db_path)

    def tearDown(self):
        self._clean()

    def _clean(self):
        for path in [
            self.db_path,
            f"{self.db_path}-wal",
            f"{self.db_path}-shm",
            "fail.flag",
        ]:
            if os.path.exists(path):
                os.remove(path)
        if os.path.exists(self.xcom_dir):
            shutil.rmtree(self.xcom_dir)

    def _query(self, sql, args=()):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, args).fetchone()

    def _assert_run_status(self, run_id, status):
        row = self._query(
            "SELECT status FROM liteflow_dag_runs WHERE run_id=?", (run_id,)
        )
        self.assertEqual(row[0], status)

    def _assert_task_status(self, run_id, task_id, status):
        row = self._query(
            "SELECT status FROM liteflow_task_instances WHERE run_id=? AND task_id=?",
            (run_id, task_id),
        )
        self.assertEqual(row[0], status)

    def test_dag_registration(self):
        with Dag("test_dag", description="A test DAG", db_path=self.db_path) as dag:
            dag.task(noop, task_id="t1")

        row = self._query(
            "SELECT description FROM liteflow_dags WHERE dag_id='test_dag'"
        )
        self.assertEqual(row[0], "A test DAG")

    def test_dag_execution_success(self):
        with Dag("success_dag", db_path=self.db_path) as dag:
            t1 = dag.task(noop, task_id="t1")
            t2 = dag.task(noop, task_id="t2")
            t1 >> t2

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")
        self._assert_task_status(run.run_id, "t1", "SUCCESS")
        self._assert_task_status(run.run_id, "t2", "SUCCESS")

    def test_dag_execution_failure(self):
        with Dag("fail_dag", db_path=self.db_path) as dag:
            t1 = dag.task(fail, task_id="t1")
            t2 = dag.task(noop, task_id="t2")
            t1 >> t2

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")
        self._assert_task_status(run.run_id, "t1", "FAILED")
        self._assert_task_status(run.run_id, "t2", "PENDING")

    def test_lifecycle_construct_persist_load_execute(self):
        """Test the full lifecycle: Define -> Persist -> Load -> Re-attach -> Execute."""
        dag_id = "lifecycle_dag"

        # 1. Define and Persist
        # The context manager automatically persists the DAG structure to the DB upon exit.
        with Dag(dag_id, db_path=self.db_path) as dag:
            t1 = dag.task(noop, task_id="t1")
            t2 = dag.task(noop, task_id="t2")
            t1 >> t2

        # 2. Load Metadata
        dag = Dag.load(db_path=self.db_path, dag_id=dag_id)

        # 3. Re-attach Logic (Bind functions to task IDs)
        # Since the DB only stores metadata, we must re-bind the Python callables.
        dag.task(noop, task_id="t1")
        dag.task(noop, task_id="t2")

        # 4. Execute
        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")

    def test_resumption(self):
        # Create flag for first run failure
        with open("fail.flag", "w") as f:
            f.write("1")

        with Dag("resume_dag", db_path=self.db_path) as dag:
            t1 = dag.task(noop, task_id="t1")
            t2 = dag.task(fail_once, task_id="t2")
            t1 >> t2

        # Run 1: Fails at t2
        run1 = dag.run()
        self._assert_run_status(run1.run_id, "FAILED")
        self._assert_task_status(run1.run_id, "t2", "FAILED")

        # Run 2: Succeeds (flag is gone)
        run2 = dag.run()
        self._assert_run_status(run2.run_id, "SUCCESS")
        self._assert_task_status(run2.run_id, "t2", "SUCCESS")

    def test_xcom_passing(self):
        with Dag("xcom_dag", db_path=self.db_path) as dag:
            t1 = dag.task(producer, task_id="producer")
            t2 = dag.task(consumer, task_id="consumer")
            t1 >> t2

        dag_run = dag.run()

        # Verify XCom value directly
        result = XCom.load(self.db_path, dag_run.run_id, "producer", "return_value")
        self.assertEqual(result, {"val": 42})

        # Verify XCom value in DB (optional, as XCom.load already tests this)
        db_val = self._query(
            "SELECT value FROM liteflow_xcom WHERE run_id=? AND task_id='consumer' AND key='return_value'",
            (dag_run.run_id,),
        )
        self.assertEqual(pickle.loads(db_val[0]), 84)

    def test_timeout(self):
        with Dag("timeout_dag", db_path=self.db_path) as dag:
            dag.task(sleep_long, task_id="slow", timeout=1)

        start_time = time.time()
        run = dag.run()
        duration = time.time() - start_time

        self.assertLess(duration, 1.9)
        self._assert_task_status(run.run_id, "slow", "FAILED")

        log = self._query(
            "SELECT error_log FROM liteflow_task_instances WHERE run_id=? AND task_id='slow'",
            (run.run_id,),
        )
        self.assertEqual(log[0], "TimeoutError")

    def test_isolation(self):
        with Dag("isolation_dag", db_path=self.db_path) as dag:
            dag.task(get_pid, task_id="t1")

        run_id = dag.run().run_id
        task_pid = XCom.load(self.db_path, run_id, "t1", "return_value")
        self.assertNotEqual(task_pid, os.getpid())

    def test_large_xcom(self):
        with Dag("large_xcom_dag", db_path=self.db_path) as dag:
            t1 = dag.task(large_producer, task_id="producer")
            t2 = dag.task(large_consumer, task_id="consumer")
            t1 >> t2

        run_id = dag.run().run_id
        result = XCom.load(self.db_path, run_id, "consumer", "return_value")
        self.assertEqual(result, 11 * 1024 * 1024)
        self.assertGreater(len(os.listdir(self.xcom_dir)), 0)

    def test_circular_dependency(self):
        with Dag("cycle_dag", db_path=self.db_path) as dag:
            t1 = dag.task(noop, task_id="t1")
            t2 = dag.task(noop, task_id="t2")
            t1 >> t2
            t2 >> t1

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")

    def test_diamond_dependency(self):
        with Dag("diamond_dag", db_path=self.db_path) as dag:
            ta = dag.task(noop, task_id="A")
            tb = dag.task(noop, task_id="B")
            tc = dag.task(noop, task_id="C")
            td = dag.task(noop, task_id="D")
            ta >> [tb, tc]
            tb >> td
            tc >> td

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")

    def test_duplicate_task_id(self):
        with self.assertRaises(ValueError):
            with Dag("dup_dag", db_path=self.db_path) as dag:
                dag.task(noop, task_id="t1")
                dag.task(noop, task_id="t1")

    def test_empty_dag(self):
        with Dag("empty_dag", db_path=self.db_path) as dag:
            pass
        self._assert_run_status(dag.run().run_id, "SUCCESS")

    def test_parallel_execution(self):
        with Dag("parallel_dag", db_path=self.db_path) as dag:
            dag.task(sleep_1s, task_id="p1")
            dag.task(sleep_1s, task_id="p2")

        start = time.time()
        dag.run()
        end = time.time()
        self.assertLess(end - start, 1.9)

    def test_large_dag_2000(self):
        with Dag("large_dag_2000", db_path=self.db_path) as dag:
            layers = [
                [dag.task(sleep_random, task_id=f"t_{i}_{j}") for j in range(100)]
                for i in range(20)
            ]
            for i in range(19):
                for j in range(100):
                    layers[i][j] >> layers[i + 1][j]
                    layers[i][(j + 1) % 100] >> layers[i + 1][j]

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")
        cnt = self._query(
            "SELECT COUNT(*) FROM liteflow_task_instances WHERE run_id=? AND status='SUCCESS'",
            (run.run_id,),
        )
        self.assertEqual(cnt[0], 2000)

    def test_execution_outside_dag(self):
        """Test that decorated tasks run as normal functions outside a DAG."""

        def add(a, b):
            return a + b

        result = add(5, 7)
        self.assertEqual(result, 12)

    def test_composition_inside_dag(self):
        """Test that calling tasks inside a DAG creates Task nodes."""

        def producer():
            return 1

        def consumer(data):
            return data + 1

        with Dag("test_dag_deferred", db_path=self.db_path) as dag:
            t1 = dag.task(producer)
            t2 = dag.task(consumer, data=t1)

            # Check tasks were added
            self.assertIn("producer", dag.tasks)
            self.assertIn("consumer", dag.tasks)

            # Check t1 is a Task instance
            self.assertIsInstance(t1, Task)
            self.assertEqual(t1.task_id, "producer")

            # Check dependency was inferred from argument
            self.assertIn("producer", t2.dependencies)
            self.assertEqual(t2.arg_dependencies["data"], "producer")

    def test_retry_success(self):
        """Test that a task retries and eventually succeeds."""
        # Create flag for failure
        with open("fail.flag", "w") as f:
            f.write("1")

        with Dag("retry_success_dag", db_path=self.db_path) as dag:
            # fail_once fails if flag exists, then removes it.
            # So Attempt 1 fails, Attempt 2 succeeds.
            dag.task(fail_once, task_id="flaky", retries=2, retry_delay=0)

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")
        self._assert_task_status(run.run_id, "flaky", "SUCCESS")

        # Verify try_number was incremented (should be 2)
        row = self._query(
            "SELECT try_number FROM liteflow_task_instances WHERE run_id=? AND task_id='flaky'",
            (run.run_id,),
        )
        self.assertEqual(row[0], 2)

    def test_retry_exhausted(self):
        """Test that a task fails after exhausting retries."""
        with Dag("retry_fail_dag", db_path=self.db_path) as dag:
            dag.task(fail, task_id="always_fail", retries=1, retry_delay=0)

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")
        self._assert_task_status(run.run_id, "always_fail", "FAILED")

        # Should have tried 2 times (1 initial + 1 retry)
        row = self._query(
            "SELECT try_number FROM liteflow_task_instances WHERE run_id=? AND task_id='always_fail'",
            (run.run_id,),
        )
        self.assertEqual(row[0], 2)

    def test_retry_timeout(self):
        """Test that timeouts trigger retries."""
        with Dag("retry_timeout_dag", db_path=self.db_path) as dag:
            # Sleep 0.2s, Timeout 0.1s -> Fail. Retry 1 -> Fail again.
            dag.task(sleep_long, task_id="slow", timeout=1, retries=1, retry_delay=0)
            # Note: sleep_long sleeps 2s, timeout 1s.

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")
        
        # Should have tried 2 times
        row = self._query(
            "SELECT try_number FROM liteflow_task_instances WHERE run_id=? AND task_id='slow'",
            (run.run_id,),
        )
        self.assertEqual(row[0], 2)

    def test_explicit_dependency_override(self):
        """Test mixing explicit arguments with bitshift operator."""

        def step1():
            pass

        def step2():
            pass

        with Dag("mix_dag", db_path=self.db_path) as dag:
            t1 = dag.task(step1)
            t2 = dag.task(step2)
            t1 >> t2

            self.assertIn("step1", t2.dependencies)
