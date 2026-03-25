#!/usr/bin/env bash
set -euo pipefail

if [[ -x "./.venv/bin/python" ]]; then
  PYTHON="./.venv/bin/python"
else
  PYTHON="${PYTHON:-python3}"
fi

exec "$PYTHON" -m pytest tests/regression -v --e2e "$@"
