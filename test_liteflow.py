import os
import pickle
import random
import shutil
import sqlite3
import time
import unittest

from liteflow import Dag, task, init_schema, XCom


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
            task(task_id="t1")(noop)

        row = self._query(
            "SELECT description FROM liteflow_dags WHERE dag_id='test_dag'"
        )
        self.assertEqual(row[0], "A test DAG")

    def test_dag_execution_success(self):
        with Dag("success_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(noop)
            t2 = task(task_id="t2")(noop)
            _ = t1 >> t2

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")
        self._assert_task_status(run.run_id, "t1", "SUCCESS")
        self._assert_task_status(run.run_id, "t2", "SUCCESS")

    def test_dag_execution_failure(self):
        with Dag("fail_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(fail)
            t2 = task(task_id="t2")(noop)
            _ = t1 >> t2

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")
        self._assert_task_status(run.run_id, "t1", "FAILED")
        self._assert_task_status(run.run_id, "t2", "PENDING")

    def test_resumption(self):
        # Create flag for first run failure
        with open("fail.flag", "w") as f:
            f.write("1")

        with Dag("resume_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(noop)
            t2 = task(task_id="t2")(fail_once)
            _ = t1 >> t2

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
            t1 = task(task_id="producer")(producer)
            t2 = task(task_id="consumer")(consumer)
            _ = t1 >> t2

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
            task(task_id="slow", timeout=1)(sleep_long)

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
            task(task_id="t1")(get_pid)

        run_id = dag.run().run_id
        task_pid = XCom.load(self.db_path, run_id, "t1", "return_value")
        self.assertNotEqual(task_pid, os.getpid())

    def test_large_xcom(self):
        with Dag("large_xcom_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="producer")(large_producer)
            t2 = task(task_id="consumer")(large_consumer)
            _ = t1 >> t2

        run_id = dag.run().run_id
        result = XCom.load(self.db_path, run_id, "consumer", "return_value")
        self.assertEqual(result, 11 * 1024 * 1024)
        self.assertGreater(len(os.listdir(self.xcom_dir)), 0)

    def test_circular_dependency(self):
        with Dag("cycle_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(noop)
            t2 = task(task_id="t2")(noop)
            _ = t1 >> t2
            _ = t2 >> t1

        run = dag.run()
        self._assert_run_status(run.run_id, "FAILED")

    def test_diamond_dependency(self):
        with Dag("diamond_dag", db_path=self.db_path) as dag:
            ta = task(task_id="A")(noop)
            tb = task(task_id="B")(noop)
            tc = task(task_id="C")(noop)
            td = task(task_id="D")(noop)
            _ = ta >> [tb, tc]
            _ = tb >> td
            _ = tc >> td

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")

    def test_duplicate_task_id(self):
        with self.assertRaises(ValueError):
            with Dag("dup_dag", db_path=self.db_path) as dag:
                task(task_id="t1")(noop)
                task(task_id="t1")(noop)

    def test_task_outside_context(self):
        with self.assertRaises(RuntimeError):

            @task(task_id="orphan")
            def orphan():
                pass

    def test_empty_dag(self):
        with Dag("empty_dag", db_path=self.db_path) as dag:
            pass
        self._assert_run_status(dag.run().run_id, "SUCCESS")

    def test_parallel_execution(self):
        with Dag("parallel_dag", db_path=self.db_path) as dag:
            task(task_id="p1")(sleep_1s)
            task(task_id="p2")(sleep_1s)

        start = time.time()
        dag.run()
        end = time.time()
        self.assertLess(end - start, 1.9)

    def test_large_dag_2000(self):
        with Dag("large_dag_2000", db_path=self.db_path) as dag:
            layers = [
                [task(task_id=f"t_{i}_{j}")(sleep_random) for j in range(100)]
                for i in range(20)
            ]
            for i in range(19):
                for j in range(100):
                    _ = layers[i][j] >> layers[i + 1][j]
                    _ = layers[i][(j + 1) % 100] >> layers[i + 1][j]

        run = dag.run()
        self._assert_run_status(run.run_id, "SUCCESS")
        cnt = self._query(
            "SELECT COUNT(*) FROM liteflow_task_instances WHERE run_id=? AND status='SUCCESS'",
            (run.run_id,),
        )
        self.assertEqual(cnt[0], 2000)
