#!/bin/sh
# 容器启动入口：先幂等初始化 SQLite 表结构，再启动主进程。
# db.create_all() 对已存在的表是 no-op，每次重启执行也安全。
set -e

python - <<'PY'
import os
from app import create_app
from app.extensions import db

app = create_app(os.getenv('FLASK_ENV', 'production'))
with app.app_context():
    db.create_all()
print("[entrypoint] SQLite 表结构已就绪")
PY

exec "$@"
