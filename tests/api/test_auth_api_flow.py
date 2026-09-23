"""认证 API 全流程合约：登录/登出/改密/忘记密码/重置/用户管理。"""

import pytest

from app.extensions import db

pytestmark = pytest.mark.module_user_auth


@pytest.fixture()
def auth_app(app):
    """在共享 app 上开启认证守卫，测试后清理测试用户。"""
    app.config["AUTH_ENABLED"] = True
    with app.app_context():
        from app.services import auth_service

        auth_service.ensure_tables()  # 共享 app 不自动建 users 表
    yield app
    with app.app_context():
        from app.models import User

        User.query.filter(User.username.like("testflow_%")).delete()
        db.session.commit()


def _make_user(app, username, email, admin=False, must_change=False):
    from app.services import auth_service

    with app.app_context():
        user = auth_service.create_user(username, email)
        user.is_admin = admin
        user.must_change_password = must_change
        db.session.commit()
        return user.id


def _inject_session(client, user_id):
    with client.session_transaction() as s:
        s["user_id"] = user_id


def test_login_success_and_me(auth_app):
    uid = _make_user(auth_app, "testflow_alice", "alice@example.com")
    client = auth_app.test_client()

    resp = client.post("/api/auth/login", json={"username": "testflow_alice", "password": "123456"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["code"] == 200
    assert data["data"]["user"]["username"] == "testflow_alice"
    assert data["data"]["must_change_password"] is False

    me = client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.get_json()["data"]["id"] == uid


def test_login_failure_uniform_message(auth_app):
    _make_user(auth_app, "testflow_bob", "bob@example.com")
    client = auth_app.test_client()

    wrong_pw = client.post("/api/auth/login", json={"username": "testflow_bob", "password": "bad"})
    unknown = client.post("/api/auth/login", json={"username": "testflow_ghost", "password": "bad"})

    # 统一文案，不泄露用户是否存在
    assert wrong_pw.status_code == 401
    assert unknown.status_code == 401
    assert wrong_pw.get_json()["message"] == unknown.get_json()["message"] == "用户名或密码错误"


def test_logout(auth_app):
    _make_user(auth_app, "testflow_carol", "carol@example.com")
    client = auth_app.test_client()
    client.post("/api/auth/login", json={"username": "testflow_carol", "password": "123456"})

    assert client.post("/api/auth/logout").status_code == 200
    assert client.get("/api/auth/me").status_code == 401


def test_change_password_flow(auth_app):
    uid = _make_user(auth_app, "testflow_dave", "dave@example.com", must_change=True)
    client = auth_app.test_client()
    client.post("/api/auth/login", json={"username": "testflow_dave", "password": "123456"})

    assert client.post("/api/auth/change-password",
                       json={"old_password": "wrong", "new_password": "newpass1"}).status_code == 400
    assert client.post("/api/auth/change-password",
                       json={"old_password": "123456", "new_password": "123"}).status_code == 400

    ok = client.post("/api/auth/change-password",
                     json={"old_password": "123456", "new_password": "newpass1"})
    assert ok.status_code == 200

    me = client.get("/api/auth/me").get_json()["data"]
    assert me["must_change_password"] is False

    # 新密码可登录
    client.post("/api/auth/logout")
    relogin = client.post("/api/auth/login",
                          json={"username": "testflow_dave", "password": "newpass1"})
    assert relogin.status_code == 200


def test_forgot_password_constant_response(auth_app):
    _make_user(auth_app, "testflow_erin", "erin@example.com")
    client = auth_app.test_client()

    known = client.post("/api/auth/forgot-password", json={"email": "erin@example.com"})
    unknown = client.post("/api/auth/forgot-password", json={"email": "ghost@example.com"})

    # 恒定文案，不泄露邮箱是否注册；SMTP 未配置时附加降级提示
    assert known.status_code == 200
    assert unknown.status_code == 200
    assert "如该邮箱已注册" in known.get_json()["message"]
    assert "如该邮箱已注册" in unknown.get_json()["message"]


def test_reset_password_with_token(auth_app):
    uid = _make_user(auth_app, "testflow_frank", "frank@example.com")
    with auth_app.app_context():
        from app.services import auth_service

        user = auth_service.get_user(uid)
        token = auth_service.generate_reset_token(user)

    client = auth_app.test_client()
    ok = client.post("/api/auth/reset-password",
                     json={"token": token, "new_password": "resetpass1"})
    assert ok.status_code == 200

    login = client.post("/api/auth/login",
                        json={"username": "testflow_frank", "password": "resetpass1"})
    assert login.status_code == 200

    # token 一次性：改密后复用失败
    reuse = client.post("/api/auth/reset-password",
                        json={"token": token, "new_password": "another1"})
    assert reuse.status_code == 400

    bad = client.post("/api/auth/reset-password",
                      json={"token": "garbage", "new_password": "resetpass1"})
    assert bad.status_code == 400


def test_users_api_admin_only(auth_app):
    normal_uid = _make_user(auth_app, "testflow_grace", "grace@example.com")
    admin_uid = _make_user(auth_app, "testflow_admin", "admin2@example.com", admin=True)
    client = auth_app.test_client()

    _inject_session(client, normal_uid)
    assert client.get("/api/auth/users").status_code == 403
    assert client.post("/api/auth/users",
                       json={"username": "x", "email": "x@example.com"}).status_code == 403

    _inject_session(client, admin_uid)
    listed = client.get("/api/auth/users")
    assert listed.status_code == 200
    usernames = [u["username"] for u in listed.get_json()["data"]["users"]]
    assert "testflow_grace" in usernames


def test_admin_create_user(auth_app):
    admin_uid = _make_user(auth_app, "testflow_admin2", "admin2@example.com", admin=True)
    client = auth_app.test_client()
    _inject_session(client, admin_uid)

    created = client.post("/api/auth/users",
                          json={"username": "testflow_heidi", "email": "heidi@example.com"})
    assert created.status_code == 200
    user = created.get_json()["data"]["user"]
    assert user["must_change_password"] is True

    # 新用户初始密码 123456 可登录
    client.post("/api/auth/logout")
    login = client.post("/api/auth/login",
                        json={"username": "testflow_heidi", "password": "123456"})
    assert login.status_code == 200

    # 用户名重复 → 400（恢复 admin 会话）
    _inject_session(client, admin_uid)
    dup = client.post("/api/auth/users",
                      json={"username": "testflow_heidi", "email": "other@example.com"})
    assert dup.status_code == 400
    assert "用户名已存在" in dup.get_json()["message"]


def test_admin_reset_other_user_password(auth_app):
    admin_uid = _make_user(auth_app, "testflow_admin3", "admin3@example.com", admin=True)
    target_uid = _make_user(auth_app, "testflow_ivan", "ivan@example.com")
    client = auth_app.test_client()
    _inject_session(client, admin_uid)

    resp = client.post(f"/api/auth/users/{target_uid}/reset-password")
    assert resp.status_code == 200
    assert "123456" in resp.get_json()["message"]

    # 被重置用户用 123456 登录且必须改密
    client.post("/api/auth/logout")
    login = client.post("/api/auth/login",
                        json={"username": "testflow_ivan", "password": "123456"})
    assert login.status_code == 200
    assert login.get_json()["data"]["must_change_password"] is True

    # 不存在的用户 → 400/404
    _inject_session(client, admin_uid)
    assert client.post("/api/auth/users/99999/reset-password").status_code in (400, 404)
