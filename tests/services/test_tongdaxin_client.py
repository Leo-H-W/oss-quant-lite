"""通达信候选服务器测速与连接回退合约。"""
import pytest

from app.services.tongdaxin import client

pytestmark = pytest.mark.module_data_jobs


HOSTS = (
    ("slow", "192.0.2.1", 7709),
    ("fast", "192.0.2.2", 7709),
    ("down", "192.0.2.3", 7709),
)


def test_rank_hosts_by_latency_puts_unreachable_hosts_last(monkeypatch):
    latency = {"slow": 0.2, "fast": 0.01, "down": None}
    monkeypatch.setattr(
        client, "_probe_host", lambda host, timeout: (host, latency[host[0]])
    )

    ranked = client.rank_hosts_by_latency(HOSTS, timeout=0.1)

    assert [host[0] for host in ranked] == ["fast", "slow", "down"]


def test_default_connection_uses_ranking_and_falls_back_to_next(monkeypatch):
    monkeypatch.setattr(
        client, "rank_hosts_by_latency", lambda hosts, timeout: [HOSTS[1], HOSTS[0]]
    )
    calls = []

    class FakeApi:
        def connect(self, host, port, time_out):
            calls.append((host, port, time_out))
            return host == HOSTS[0][1]

    selected = client.connect_first_available(FakeApi())

    assert selected == HOSTS[0]
    assert [call[0] for call in calls] == [HOSTS[1][1], HOSTS[0][1]]


def test_explicit_hosts_keep_caller_order_without_probing(monkeypatch):
    def unexpected_probe(*_args, **_kwargs):
        raise AssertionError("显式分片主机不应被重新测速排序")

    monkeypatch.setattr(client, "rank_hosts_by_latency", unexpected_probe)

    class FakeApi:
        def connect(self, *_args, **_kwargs):
            return True

    assert client.connect_first_available(FakeApi(), hosts=HOSTS) == HOSTS[0]


def test_all_connections_failed_raises_visible_error(monkeypatch):
    monkeypatch.setattr(client, "rank_hosts_by_latency", lambda hosts, timeout: list(HOSTS))

    class FakeApi:
        def connect(self, *_args, **_kwargs):
            return False

    with pytest.raises(ConnectionError, match="No available pytdx hosts"):
        client.connect_first_available(FakeApi())


def test_hq_api_raises_request_errors_instead_of_returning_none(monkeypatch):
    options = []

    class FakeTdxApi:
        def __init__(self, **kwargs):
            options.append(kwargs)

    monkeypatch.setattr("pytdx.hq.TdxHq_API", FakeTdxApi)
    client.create_hq_api()

    assert options[0]["raise_exception"] is True
    assert options[0]["auto_retry"] is True
