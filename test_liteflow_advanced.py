import os
import sqlite3
import time
import unittest

from liteflow import DAG, task, LiteFlowDB


def slow_task_func():
    time.sleep(5)
    return "done"


def get_pid_func():
    return os.getpid()


# Tasks for tests
def slow_task_wrapper():
    return slow_task_func()


def get_pid_wrapper():
    return get_pid_func()


def large_producer():
    return "a" * (11 * 1024 * 1024)


def large_consumer(producer):
    return len(producer)


class TestLiteFlowAdvanced(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_advanced.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_timeout(self):
        # This test should verify that a task timing out is marked as FAILED
        with DAG("timeout_dag", db_path=self.db_path) as dag:
            task(task_id="slow_task", timeout=1)(slow_task_wrapper)

        start_time = time.time()
        run_id = dag.run()
        end_time = time.time()

        # It should have failed around 1 second, not 5
        self.assertLess(end_time - start_time, 4)

        # Check status in DB
        db = LiteFlowDB(self.db_path)
        states = db.get_task_states(run_id)
        self.assertEqual(states["slow_task"], "FAILED")

        # Check error log
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT error_log FROM task_instances WHERE run_id=? AND task_id='slow_task'",
            (run_id,),
        )
        log = cursor.fetchone()[0]
        self.assertEqual(log, "TimeoutError")
        conn.close()

    def test_isolation(self):
        # This test verifies that tasks run in different processes
        main_pid = os.getpid()

        with DAG("isolation_dag", db_path=self.db_path) as dag:
            task(task_id="t1")(get_pid_wrapper)

        run_id = dag.run()

        db = LiteFlowDB(self.db_path)
        task_pid = db.get_xcom(run_id, "t1", "return_value")

        self.assertIsNotNone(task_pid)
        self.assertNotEqual(task_pid, main_pid)

    def test_large_xcom(self):
        with DAG("large_xcom_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="producer")(large_producer)
            t2 = task(task_id="consumer")(large_consumer)
            t1 >> t2

        run_id = dag.run()

        db = LiteFlowDB(self.db_path)
        result = db.get_xcom(run_id, "consumer", "return_value")
        self.assertEqual(result, 11 * 1024 * 1024)

        # Verify file exists
        xcom_dir = self.db_path + "_xcom"
        self.assertTrue(os.path.exists(xcom_dir))
        self.assertGreater(len(os.listdir(xcom_dir)), 0)

        # Cleanup xcom dir
        import shutil

        if os.path.exists(xcom_dir):
            shutil.rmtree(xcom_dir)


if __name__ == "__main__":
    unittest.main()
