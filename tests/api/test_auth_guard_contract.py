"""全站登录守卫合约：未登录 302/401、白名单、强制改密拦截、开关豁免。"""

import pytest

from app.extensions import db

pytestmark = pytest.mark.module_user_auth


@pytest.fixture()
def auth_app(app):
    app.config["AUTH_ENABLED"] = True
    with app.app_context():
        from app.services import auth_service

        auth_service.ensure_tables()  # 共享 app 不自动建 users 表
    yield app
    with app.app_context():
        from app.models import User

        User.query.filter(User.username.like("testguard_%")).delete()
        db.session.commit()


def _make_user(app, username, must_change=False):
    from app.services import auth_service

    with app.app_context():
        user = auth_service.create_user(username, f"{username}@example.com")
        user.must_change_password = must_change
        db.session.commit()
        return user.id


def _inject_session(client, user_id):
    with client.session_transaction() as s:
        s["user_id"] = user_id


def test_page_request_redirects_to_login(auth_app):
    client = auth_app.test_client()
    resp = client.get("/")
    assert resp.status_code == 302
    location = resp.headers["Location"]
    assert "/login" in location
    assert "next=" in location


def test_api_request_returns_401_json(auth_app):
    client = auth_app.test_client()
    resp = client.get("/api/auth/me")
    assert resp.status_code == 401
    data = resp.get_json()
    assert data["code"] == 401
    assert data["data"] is None


def test_whitelist_paths_not_intercepted(auth_app):
    client = auth_app.test_client()

    assert client.get("/login").status_code == 200
    assert client.get("/forgot-password").status_code == 200
    assert client.get("/reset-password/faketoken").status_code == 200
    # 静态资源与 websocket 握手路径不 302
    assert client.get("/static/css/financial-theme.css").status_code != 302
    assert client.get("/socket.io/").status_code != 302
    # 匿名可调用的认证 API
    assert client.post("/api/auth/login", json={}).status_code != 302
    assert client.post("/api/auth/forgot-password", json={}).status_code != 302


def test_must_change_password_intercept(auth_app):
    uid = _make_user(auth_app, "testguard_newbie", must_change=True)
    client = auth_app.test_client()
    _inject_session(client, uid)

    # 页面 → 302 到强制改密页
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/change-password" in resp.headers["Location"]

    # 其他 API → 403 并提示
    api = client.get("/api/workbench/cards")
    assert api.status_code == 403
    assert api.get_json()["data"]["must_change_password"] is True

    # 放行路径：改密页、改密 API、me、logout
    assert client.get("/change-password").status_code == 200
    assert client.get("/api/auth/me").status_code == 200
    assert client.post("/api/auth/logout").status_code == 200


def test_normal_user_passes_guard(auth_app):
    uid = _make_user(auth_app, "testguard_normal", must_change=False)
    client = auth_app.test_client()
    _inject_session(client, uid)

    assert client.get("/").status_code == 200
    assert client.get("/api/auth/me").status_code == 200


def test_auth_disabled_passes_through(auth_app):
    auth_app.config["AUTH_ENABLED"] = False
    client = auth_app.test_client()

    assert client.get("/").status_code == 200
    assert client.get("/api/auth/me").status_code != 302


def test_stale_session_cleared(auth_app):
    """session 里的 user_id 已不存在时视为未登录并清 session。"""
    client = auth_app.test_client()
    _inject_session(client, 99999)

    resp = client.get("/")
    assert resp.status_code == 302
    with client.session_transaction() as s:
        assert "user_id" not in s
