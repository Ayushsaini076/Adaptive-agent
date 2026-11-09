#!/usr/bin/env bash
# Usage: ./run.sh <SIM_URL>
# Starts the controller against an already-running simulator.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <SIM_URL>" >&2
  exit 2
fi

cd "$(dirname "$0")"

if [[ ! -x .venv/bin/python ]]; then
  python3 -m venv .venv
  .venv/bin/pip install -q -r requirements.txt
fi

exec .venv/bin/python -m controller.main "$1"
