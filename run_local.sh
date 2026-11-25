#!/usr/bin/env bash
# 本地(无 docker)启动: 优先 venv, 否则使用项目内 .pylibs
set -euo pipefail
cd "$(dirname "$0")"
if [ -d ".venv" ]; then
  . .venv/bin/activate
elif [ -d ".pylibs" ]; then
  export PYTHONPATH="$(pwd)/.pylibs${PYTHONPATH:+:$PYTHONPATH}"
fi
exec python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 "$@"
