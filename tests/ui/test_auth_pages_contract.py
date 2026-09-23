"""认证页面合约：登录页独立、改密页、用户管理页（admin）、导航用户菜单。"""

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

        User.query.filter(User.username.like("testui_%")).delete()
        db.session.commit()


def _make_user(app, username, admin=False):
    from app.services import auth_service

    with app.app_context():
        user = auth_service.create_user(username, f"{username}@example.com")
        user.is_admin = admin
        user.must_change_password = False
        db.session.commit()
        return user.id


def _inject_session(client, user_id):
    with client.session_transaction() as s:
        s["user_id"] = user_id


def test_login_page_is_standalone(auth_app):
    client = auth_app.test_client()
    resp = client.get("/login")

    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    # 登录页是独立页：有表单，无主导航
    assert 'name="username"' in html
    assert 'name="password"' in html or 'type="password"' in html
    assert "navbar-financial" not in html
    # 提供忘记密码入口
    assert "forgot-password" in html


def test_forgot_and_reset_pages_standalone(auth_app):
    client = auth_app.test_client()

    forgot = client.get("/forgot-password")
    assert forgot.status_code == 200
    assert "navbar-financial" not in forgot.get_data(as_text=True)

    reset = client.get("/reset-password/sometoken")
    assert reset.status_code == 200
    html = reset.get_data(as_text=True)
    assert "navbar-financial" not in html
    assert "sometoken" in html  # token 渲染进页面供提交


def test_change_password_page(auth_app):
    uid = _make_user(auth_app, "testui_changer")
    client = auth_app.test_client()
    _inject_session(client, uid)

    resp = client.get("/change-password")
    assert resp.status_code == 200
    assert "password" in resp.get_data(as_text=True)


def test_user_management_requires_admin(auth_app):
    normal_uid = _make_user(auth_app, "testui_normal")
    admin_uid = _make_user(auth_app, "testui_admin", admin=True)
    client = auth_app.test_client()

    _inject_session(client, normal_uid)
    assert client.get("/users").status_code == 403

    _inject_session(client, admin_uid)
    resp = client.get("/users")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "用户管理" in html
    assert "navbar-financial" in html  # 继承 base.html 带导航


def test_base_pages_show_user_menu(auth_app):
    uid = _make_user(auth_app, "testui_viewer")
    client = auth_app.test_client()
    _inject_session(client, uid)

    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "navbar-user-menu" in html
    assert "testui_viewer" in html  # 显示当前用户名
    assert "退出登录" in html


def test_admin_sees_user_management_menu(auth_app):
    uid = _make_user(auth_app, "testui_adminmenu", admin=True)
    client = auth_app.test_client()
    _inject_session(client, uid)

    html = client.get("/").get_data(as_text=True)
    assert "用户管理" in html
    assert "/users" in html
