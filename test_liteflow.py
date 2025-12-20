import os
import sqlite3
import unittest

from liteflow import DAG, task, LiteFlowDB


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


class TestLiteFlow(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_liteflow.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists("fail_once.flag"):
            os.remove("fail_once.flag")
        self.db = LiteFlowDB(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists("fail_once.flag"):
            os.remove("fail_once.flag")

    def test_dag_registration(self):
        with DAG("test_dag", description="A test DAG", db_path=self.db_path) as dag:

            @task(task_id="t1")
            def t1():
                return "hello"

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT dag_id, description FROM dags WHERE dag_id='test_dag'")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "test_dag")
        self.assertEqual(row[1], "A test DAG")
        conn.close()

    def test_dag_execution_success(self):
        with DAG("success_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_success)
            t2 = task(task_id="t2")(t2_success)
            t1 >> t2

        dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM dag_runs WHERE dag_id='success_dag'")
        status = cursor.fetchone()[0]
        self.assertEqual(status, "SUCCESS")

        cursor.execute(
            "SELECT task_id, status FROM task_instances WHERE run_id=(SELECT run_id FROM dag_runs WHERE dag_id='success_dag')"
        )
        rows = cursor.fetchall()
        task_statuses = {row[0]: row[1] for row in rows}
        self.assertEqual(task_statuses["t1"], "SUCCESS")
        self.assertEqual(task_statuses["t2"], "SUCCESS")
        conn.close()

    def test_dag_execution_failure(self):
        with DAG("fail_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_fail)

            @task(task_id="t2")
            def t2():
                pass

            t1 >> t2

        dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM dag_runs WHERE dag_id='fail_dag'")
        status = cursor.fetchone()[0]
        self.assertEqual(status, "FAILED")

        cursor.execute(
            "SELECT task_id, status FROM task_instances WHERE run_id=(SELECT run_id FROM dag_runs WHERE dag_id='fail_dag')"
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

        with DAG("resume_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="t1")(t1_resume)
            t2 = task(task_id="t2")(t2_resume)
            t1 >> t2

        run_id = dag.run()

        # Verify failure
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status, error_log FROM task_instances WHERE run_id=? AND task_id='t1'",
            (run_id,),
        )
        row1 = cursor.fetchone()
        self.assertEqual(row1[0], "SUCCESS", f"t1 failed: {row1[1]}")
        cursor.execute(
            "SELECT status, error_log FROM task_instances WHERE run_id=? AND task_id='t2'",
            (run_id,),
        )
        row2 = cursor.fetchone()
        self.assertEqual(row2[0], "FAILED")
        conn.close()

        # Second run succeeds
        dag.run(run_id=run_id)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM dag_runs WHERE run_id=?", (run_id,))
        self.assertEqual(cursor.fetchone()[0], "SUCCESS")

        cursor.execute(
            "SELECT status FROM task_instances WHERE run_id=? AND task_id='t2'",
            (run_id,),
        )
        self.assertEqual(cursor.fetchone()[0], "SUCCESS")
        conn.close()

    def test_xcom_passing(self):
        with DAG("xcom_dag", db_path=self.db_path) as dag:
            t1 = task(task_id="producer")(producer_func)
            t2 = task(task_id="consumer")(consumer_func)
            t1 >> t2

        run_id = dag.run()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check producer output
        cursor.execute(
            "SELECT value FROM xcom WHERE run_id=? AND task_id='producer' AND key='return_value'",
            (run_id,),
        )
        import pickle

        val = pickle.loads(cursor.fetchone()[0])
        self.assertEqual(val, {"data": 42})

        # Check consumer output
        cursor.execute(
            "SELECT value FROM xcom WHERE run_id=? AND task_id='consumer' AND key='return_value'",
            (run_id,),
        )
        val = pickle.loads(cursor.fetchone()[0])
        self.assertEqual(val, 84)
        conn.close()


if __name__ == "__main__":
    unittest.main()
