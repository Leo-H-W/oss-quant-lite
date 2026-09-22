FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# 统一容器时区为北京时间：内部时间戳与 A 股行情数据的时区语义一致
ENV TZ=Asia/Shanghai
# 应用监听端口（gunicorn bind / EXPOSE / HEALTHCHECK 保持一致）
ENV PORT=9090
# apt 换阿里云源（Debian trixie 使用 deb822 格式源文件）；
# pip 走阿里云镜像，国内服务器构建快一个数量级；构建海外镜像时可 --build-arg 覆盖
ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

RUN sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements_minimal.txt ./
# minimal 提供 gunicorn/eventlet/cvxpy 等运行时底座，
# 全量 requirements 补齐 jieba/xgboost/lightgbm/tushare/baostock/pytdx
# （app/services 顶层 import，缺了容器启动即 ModuleNotFoundError）
RUN pip install --no-cache-dir -r requirements_minimal.txt -r requirements.txt

COPY . .

RUN mkdir -p /app/instance /app/data /app/logs \
    && useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app \
    && chmod +x /app/docker-entrypoint.sh
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:9090/', timeout=3).status < 500 else 1)" || exit 1

# 启动前幂等建表，再执行 CMD
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# 生产：gunicorn + eventlet worker（自动 monkey_patch，支撑 WebSocket）
CMD ["gunicorn", "--worker-class", "eventlet", "--workers", "1", "--bind", "0.0.0.0:9090", "--timeout", "300", "run:app"]
