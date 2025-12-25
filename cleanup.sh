#!/usr/bin/env bash
set -euo pipefail

# Find and remove SQLite related files recursively
find . -type f \( -name "*.db" -o -name "*.db-shm" -o -name "*.db-wal" \) -print -delete
