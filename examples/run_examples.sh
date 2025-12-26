#!/usr/bin/env bash
#set -euo pipefail

rm -rf examples.db examples.db-shm examples.db-wal;
python simple_dag_run.py;
python simple_dag_with_xcom.py;
python persist_first_then_run.py;
