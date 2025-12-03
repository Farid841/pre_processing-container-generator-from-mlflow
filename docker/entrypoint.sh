#!/bin/bash
set -e

# Le preprocessing est déjà dans /app/preprocessing/preprocessing.py
# On exécute simplement le runner

python /app/runner/runner.py
