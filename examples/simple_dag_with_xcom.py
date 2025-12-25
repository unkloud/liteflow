import os
from liteflow import Dag, task, init_schema
from pathlib import Path
import sys

python_path = Path(__file__).parent.resolve()
sys.path.insert(0, str(python_path))


def fetch_user_data():
    """Simulates fetching data from an API."""
    print("Task 1: Fetching data...")
    return {"user_id": 42, "username": "jdoe", "email": "jdoe@example.com"}


def validate_user(user_profile):
    """
    Validates the user data.
    The argument 'user_profile' matches the task_id of the upstream task.
    """
    print(f"Task 2: Validating user {user_profile['username']}...")
    if "@" not in user_profile["email"]:
        raise ValueError("Invalid email")
    return True


def send_welcome_email(user_profile, validation_result):
    """
    Sends an email if validation passed.
    Depends on 'user_profile' (data) and 'validation_result' (status).
    """
    if validation_result:
        print(f"Task 3: Sending welcome email to {user_profile['email']}...")
        return "Email Sent"
    return "Skipped"


# --- Main Execution ---
if __name__ == "__main__":
    db_path = "xcom_demo.db"

    # Clean up previous run for this demo
    if os.path.exists(db_path):
        os.remove(db_path)

    # Initialize the database schema
    init_schema(db_path)

    # Create the DAG
    with Dag("onboarding_flow", db_path=db_path) as dag:
        # Define tasks with IDs matching the arguments of downstream functions
        t1 = task(task_id="user_profile")(fetch_user_data)
        t2 = task(task_id="validation_result")(validate_user)
        t3 = task(task_id="email_status")(send_welcome_email)

        # Define dependencies
        # t2 needs t1's output
        _ = t1 >> t2

        # t3 needs both t1 and t2
        _ = t1 >> t3
        _ = t2 >> t3

    # Run the DAG
    print("Running DAG...")
    run = dag.run()
    print(f"DAG Run Status: {run.status}")
