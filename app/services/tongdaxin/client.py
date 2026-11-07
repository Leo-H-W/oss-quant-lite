from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import time


# 在原有 4 台基础上补充 easy-tdx 使用的公共候选；显式 hosts 仍保持调用方顺序。
DEFAULT_HOSTS = (
    ("default", "119.147.212.81", 7709),
    ("server_1", "115.238.56.198", 7709),
    ("server_2", "115.238.90.165", 7709),
    ("server_3", "180.153.18.170", 7709),
    ("server_4", "180.153.18.171", 7709),
    ("server_5", "180.153.18.172", 7709),
    ("server_6", "111.229.247.189", 7709),
    ("server_7", "150.158.160.2", 7709),
    ("server_8", "124.71.187.122", 7709),
    ("server_9", "218.75.126.9", 7709),
    ("server_10", "47.107.75.159", 7709),
    ("server_11", "59.175.238.38", 7709),
)
CONNECT_TIMEOUT = 5.0
PROBE_TIMEOUT = 1.0
PROBE_WORKERS = 8


def create_hq_api(**overrides):
    from pytdx.hq import TdxHq_API

    options = {
        "heartbeat": True,
        "auto_retry": True,
        "raise_exception": True,
    }
    options.update(overrides)
    return TdxHq_API(**options)


def normalize_hosts(hosts: Iterable[tuple[str, str, int]] | None = None) -> list[tuple[str, str, int]]:
    normalized = []
    for name, host, port in (hosts or DEFAULT_HOSTS):
        normalized.append((str(name), str(host), int(port)))
    return normalized


def _probe_host(host: tuple[str, str, int], timeout: float):
    """用真实 pytdx 握手测速；无论成败都释放探测连接。"""
    api = create_hq_api(heartbeat=False, auto_retry=False, raise_exception=True)
    started = time.monotonic()
    try:
        if api.connect(host[1], host[2], time_out=timeout):
            return host, time.monotonic() - started
    except Exception:  # noqa: BLE001 - 单台不可达不影响其他候选
        pass
    finally:
        try:
            api.disconnect()
        except Exception:  # noqa: BLE001
            pass
    return host, None


def rank_hosts_by_latency(
    hosts: Iterable[tuple[str, str, int]] | None = None,
    timeout: float = PROBE_TIMEOUT,
) -> list[tuple[str, str, int]]:
    """并发握手测速，可达主机按延迟排序，不可达主机保留在最后作兜底。"""
    candidates = normalize_hosts(hosts)
    if not candidates:
        return []
    with ThreadPoolExecutor(max_workers=min(PROBE_WORKERS, len(candidates))) as pool:
        futures = [pool.submit(_probe_host, host, timeout) for host in candidates]
        results = []
        for index, future in enumerate(futures):
            try:
                host, latency = future.result()
            except Exception:  # noqa: BLE001 - 探测异常按不可达处理
                host, latency = candidates[index], None
            results.append((host, latency, index))
    results.sort(key=lambda item: (item[1] is None, item[1] or 0.0, item[2]))
    return [host for host, _latency, _index in results]


def connect_first_available(api, hosts: Iterable[tuple[str, str, int]] | None = None) -> tuple[str, str, int]:
    # 显式 hosts 用于回补分片钉住服务器；只有默认路径进行并发测速排序。
    candidates = (
        rank_hosts_by_latency(None, timeout=PROBE_TIMEOUT)
        if hosts is None else normalize_hosts(hosts)
    )
    last_error = None
    for name, host, port in candidates:
        try:
            if api.connect(host, port, time_out=CONNECT_TIMEOUT):
                return name, host, port
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise ConnectionError("No available pytdx hosts") from last_error
    raise ConnectionError("No available pytdx hosts")


@contextmanager
def connected_session(api, hosts: Iterable[tuple[str, str, int]] | None = None):
    connect_first_available(api, hosts)
    try:
        yield api
    finally:
        api.disconnect()
