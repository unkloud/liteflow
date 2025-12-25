import os
import pickle
import random
import shutil
import sqlite3
import time
import unittest

from liteflow import Dag, task, LiteFlowDB, get_xcom


# Helper functions for tests (must be top-level for pickling)
def t1_success():
    pass


def t2_success():
    pass


def t1_fail():
    raise Exception("Boom!")


def t1_resume():
    pass


def t2_resume(fail_flag=None):
    # Note: we can't easily use global fail_flag here because it's a different process
    # Instead, we can use a file or just rely on the test passing it if we support it.
    # But XCom is better.
    if os.path.exists("fail_once.flag"):
        os.remove("fail_once.flag")
        raise Exception("Fail once")


def producer_func():
    return {"data": 42}


def consumer_func(producer):
    return producer["data"] * 2


def slow_task_func():
    time.sleep(5)
    return "done"


def get_pid_func():
    return os.getpid()


def slow_task_wrapper():
    return slow_task_func()


def get_pid_wrapper():
    return get_pid_func()


def large_producer():
    return "a" * (11 * 1024 * 1024)


def large_consumer(producer):
    return len(producer)


def sleep_1():
    time.sleep(1)


def random_sleep_task():
    time.sleep(random.uniform(0.1, 0.8))


class TestLiteFlow(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_liteflow.db"
        self.xcom_dir = self.db_path + "_xcom"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists("fail_once.flag"):
            os.remove("fail_once.flag")
        if os.path.exists(self.xcom_dir):
            shutil.rmtree(self.xcom_dir)
        self.db = LiteFlowDB(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists("fail_once.flag"):
            os.remove("fail_once.flag")
        if os.path.exists(self.xcom_dir):
            shutil.rmtree(self.xcom_dir)

    def test_dag_registration(self):
        with Dag("test_dag", description="A test DAG", db_path=self.db_path) as dag:

            @task(task_id="t1")
            def t1():
                return "hello"

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT dag_id, description FROM liteflow_dags WHERE dag_id='test_dag'"
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "test_dag")
        self.assertEqual(row[1], "A test DAG")
        conn.close()

    def test_dag_execution_success(self):
        with Dag("success_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_success)
            t2 = task(task_id="t2")(t2_success)
            t1 >> t2

        dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM liteflow_dag_runs WHERE dag_id='success_dag'"
        )
        status = cursor.fetchone()[0]
        self.assertEqual(status, "SUCCESS")

        cursor.execute(
            "SELECT task_id, status FROM liteflow_task_instances WHERE run_id=(SELECT run_id FROM liteflow_dag_runs WHERE dag_id='success_dag')"
        )
        rows = cursor.fetchall()
        task_statuses = {row[0]: row[1] for row in rows}
        self.assertEqual(task_statuses["t1"], "SUCCESS")
        self.assertEqual(task_statuses["t2"], "SUCCESS")
        conn.close()

    def test_dag_execution_failure(self):
        with Dag("fail_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_fail)

            @task(task_id="t2")
            def t2():
                pass

            t1 >> t2

        dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM liteflow_dag_runs WHERE dag_id='fail_dag'")
        status = cursor.fetchone()[0]
        self.assertEqual(status, "FAILED")

        cursor.execute(
            "SELECT task_id, status FROM liteflow_task_instances WHERE run_id=(SELECT run_id FROM liteflow_dag_runs WHERE dag_id='fail_dag')"
        )
        rows = cursor.fetchall()
        task_statuses = {row[0]: row[1] for row in rows}
        self.assertEqual(task_statuses["t1"], "FAILED")
        self.assertEqual(task_statuses["t2"], "PENDING")  # Should not run
        conn.close()

    def test_resumption(self):
        # First run fails
        with open("fail_once.flag", "w") as f:
            f.write("1")

        with Dag("resume_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_resume)
            t2 = task(task_id="t2")(t2_resume)
            t1 >> t2

        run_id = dag.run()

        # Verify failure
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, error_log FROM liteflow_task_instances WHERE run_id=? AND task_id='t1'",
            (run_id,),
        )
        row1 = cursor.fetchone()
        self.assertEqual(row1[0], "SUCCESS", f"t1 failed: {row1[1]}")
        cursor.execute(
            "SELECT status, error_log FROM liteflow_task_instances WHERE run_id=? AND task_id='t2'",
            (run_id,),
        )
        row2 = cursor.fetchone()
        self.assertEqual(row2[0], "FAILED")
        conn.close()

        # Second run succeeds
        dag.run(run_id=run_id)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM liteflow_dag_runs WHERE run_id=?", (run_id,))
        self.assertEqual(cursor.fetchone()[0], "SUCCESS")

        cursor.execute(
            "SELECT status FROM liteflow_task_instances WHERE run_id=? AND task_id='t2'",
            (run_id,),
        )
        self.assertEqual(cursor.fetchone()[0], "SUCCESS")
        conn.close()

    def test_xcom_passing(self):
        with Dag("xcom_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="producer")(producer_func)
            t2 = task(task_id="consumer")(consumer_func)
            t1 >> t2

        run_id = dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check producer output
        cursor.execute(
            "SELECT value FROM liteflow_xcom WHERE run_id=? AND task_id='producer' AND key='return_value'",
            (run_id,),
        )
        val = pickle.loads(cursor.fetchone()[0])
        self.assertEqual(val, {"data": 42})

        # Check consumer output
        cursor.execute(
            "SELECT value FROM liteflow_xcom WHERE run_id=? AND task_id='consumer' AND key='return_value'",
            (run_id,),
        )
        val = pickle.loads(cursor.fetchone()[0])
        self.assertEqual(val, 84)
        conn.close()

    def test_timeout(self):
        # This test should verify that a task timing out is marked as FAILED
        with Dag("timeout_dag", db_path=self.db_path) as dag:
            task(task_id="slow_task", timeout=1)(slow_task_wrapper)

        start_time = time.time()
        run_id = dag.run()
        end_time = time.time()

        # It should have failed around 1 second, not 5
        self.assertLess(end_time - start_time, 4)

        # Check status in DB
        states = self.db.get_task_states(run_id)
        self.assertEqual(states["slow_task"], "FAILED")

        # Check error log
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT error_log FROM liteflow_task_instances WHERE run_id=? AND task_id='slow_task'",
            (run_id,),
        )
        log = cursor.fetchone()[0]
        self.assertEqual(log, "TimeoutError")
        conn.close()

    def test_isolation(self):
        # This test verifies that tasks run in different processes
        main_pid = os.getpid()

        with Dag("isolation_dag", db_path=self.db_path) as dag:
            task(task_id="t1")(get_pid_wrapper)

        run_id = dag.run()
        task_pid = get_xcom(self.db_path, run_id, "t1", "return_value")

        self.assertIsNotNone(task_pid)
        self.assertNotEqual(task_pid, main_pid)

    def test_large_xcom(self):
        with Dag("large_xcom_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="producer")(large_producer)
            t2 = task(task_id="consumer")(large_consumer)
            t1 >> t2

        run_id = dag.run()
        result = get_xcom(self.db_path, run_id, "consumer", "return_value")
        self.assertEqual(result, 11 * 1024 * 1024)

        # Verify file exists
        self.assertTrue(os.path.exists(self.xcom_dir))
        self.assertGreater(len(os.listdir(self.xcom_dir)), 0)

    def test_circular_dependency(self):
        with Dag("cycle_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_success)
            t2 = task(task_id="t2")(t1_success)
            t1 >> t2
            t2 >> t1

        run_id = dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM liteflow_dag_runs WHERE run_id=?", (run_id,))
        status = cursor.fetchone()[0]
        self.assertEqual(status, "FAILED")
        conn.close()

    def test_diamond_dependency(self):
        with Dag("diamond_dag", db_path=self.db_path) as dag:
            # A -> B -> D
            # A -> C -> D
            ta = task(task_id="A")(t1_success)
            tb = task(task_id="B")(t1_success)
            tc = task(task_id="C")(t1_success)
            td = task(task_id="D")(t1_success)

            ta >> [tb, tc]
            tb >> td
            tc >> td

        run_id = dag.run()

        states = self.db.get_task_states(run_id)
        for tid in ["A", "B", "C", "D"]:
            self.assertEqual(states[tid], "SUCCESS")

    def test_duplicate_task_id(self):
        with self.assertRaises(ValueError):
            with Dag("dup_dag", db_path=self.db_path) as dag:
                task(task_id="t1")(t1_success)
                task(task_id="t1")(t1_success)

    def test_task_outside_context(self):
        with self.assertRaises(RuntimeError):

            @task(task_id="orphan")
            def orphan():
                pass

    def test_empty_dag(self):
        with Dag("empty_dag", db_path=self.db_path) as dag:
            pass

        run_id = dag.run()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM liteflow_dag_runs WHERE run_id=?", (run_id,))
        status = cursor.fetchone()[0]
        self.assertEqual(status, "SUCCESS")
        conn.close()

    def test_parallel_execution(self):
        # Two tasks sleeping 1s each. In parallel, should take ~1s, definitely < 2s.
        with Dag("parallel_dag", db_path=self.db_path) as dag:
            task(task_id="p1")(sleep_1)
            task(task_id="p2")(sleep_1)

        start = time.time()
        dag.run()
        end = time.time()

        duration = end - start
        # 2.0 would be serial execution time (approx).
        # We expect parallel to be faster.
        self.assertLess(duration, 1.9)

    def test_large_dag_2000(self):
        with Dag("large_dag_2000", db_path=self.db_path) as dag:
            # Create a multi-stage DAG with 2000 tasks
            # 20 layers of 100 tasks each
            layers = []
            for i in range(20):
                layer = [
                    task(task_id=f"t_{i}_{j}")(random_sleep_task) for j in range(100)
                ]
                layers.append(layer)

            # Connect layers to form a mesh
            for i in range(19):
                for j in range(100):
                    # Each task depends on two tasks from the previous layer
                    layers[i][j] >> layers[i + 1][j]
                    layers[i][(j + 1) % 100] >> layers[i + 1][j]

        run_id = dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM liteflow_dag_runs WHERE run_id=?", (run_id,))
        self.assertEqual(cursor.fetchone()[0], "SUCCESS")

        cursor.execute(
            "SELECT count(*) FROM liteflow_task_instances WHERE run_id=? AND status='SUCCESS'",
            (run_id,),
        )
        self.assertEqual(cursor.fetchone()[0], 2000)
        conn.close()


if __name__ == "__main__":
    unittest.main()
