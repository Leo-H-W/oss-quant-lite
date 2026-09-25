#!/usr/bin/env bash
# 服务管理脚本：start | stop | restart | status
set -u

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$APP_DIR/logs/run.pid"
LOG_FILE="$APP_DIR/logs/server.log"
PYTHON="$APP_DIR/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

# 从 .env 读取端口（默认 9090）
PORT=$(grep -E '^PORT=' "$APP_DIR/.env" 2>/dev/null | tail -1 | cut -d= -f2 | tr -d '[:space:]')
PORT=${PORT:-9090}
URL="http://localhost:${PORT}"

get_pid() {
    # 优先 pid 文件，其次按进程命令行匹配
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
        rm -f "$PID_FILE"
    fi
    pgrep -f "python.*run\.py" | head -1
}

do_status() {
    local pid
    pid=$(get_pid)
    if [ -n "$pid" ]; then
        echo "服务运行中 (PID: $pid)"
        echo "访问地址: $URL"
        return 0
    else
        echo "服务未运行"
        return 1
    fi
}

do_start() {
    local pid
    pid=$(get_pid)
    if [ -n "$pid" ]; then
        echo "服务已在运行 (PID: $pid)，访问地址: $URL"
        return 0
    fi
    mkdir -p "$APP_DIR/logs"
    cd "$APP_DIR" || exit 1
    nohup "$PYTHON" run.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    sleep 2
    pid=$(get_pid)
    if [ -n "$pid" ]; then
        echo "服务启动成功 (PID: $pid)"
        echo "访问地址: $URL"
        echo "日志文件: $LOG_FILE"
    else
        echo "服务启动失败，请查看日志: $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

do_stop() {
    local pid
    pid=$(get_pid)
    if [ -z "$pid" ]; then
        echo "服务未运行"
        return 0
    fi
    kill "$pid"
    for _ in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "优雅停止超时，强制结束 (PID: $pid)"
        kill -9 "$pid"
    fi
    rm -f "$PID_FILE"
    echo "服务已停止"
}

case "${1:-}" in
    start)   do_start ;;
    stop)    do_stop ;;
    restart) do_stop; do_start ;;
    status)  do_status ;;
    *)
        echo "用法: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac
