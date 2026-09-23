"""用户认证服务合约：密码哈希、用户创建、改密、管理员重置、admin 种子。"""

import pytest
from flask import Flask

from app.extensions import db
from app.models import User  # noqa: F401  确保建表前模型已注册到 metadata

pytestmark = pytest.mark.module_user_auth


@pytest.fixture()
def app(tmp_path):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY="test-secret",
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{tmp_path}/svc_users.db",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()


def test_create_user_defaults(app):
    from app.services import auth_service

    user = auth_service.create_user("alice", "alice@example.com")

    assert user.username == "alice"
    assert user.email == "alice@example.com"
    assert user.is_admin is False
    # 新用户初始密码 123456，且首次登录强制改密
    assert user.must_change_password is True
    assert user.check_password(auth_service.INITIAL_PASSWORD)
    # 哈希落库，不存明文
    assert user.password_hash != auth_service.INITIAL_PASSWORD


def test_duplicate_username_rejected(app):
    from app.services import auth_service

    auth_service.create_user("bob", "bob@example.com")
    with pytest.raises(ValueError, match="用户名已存在"):
        auth_service.create_user("bob", "other@example.com")


def test_authenticate(app):
    from app.services import auth_service

    auth_service.create_user("carol", "carol@example.com")

    assert auth_service.authenticate("carol", "123456") is not None
    # 错误密码与不存在用户都返回 None（调用方统一报错，防用户枚举）
    assert auth_service.authenticate("carol", "wrong") is None
    assert auth_service.authenticate("nobody", "123456") is None


def test_change_password_flow(app):
    from app.services import auth_service

    user = auth_service.create_user("dave", "dave@example.com")

    with pytest.raises(ValueError):
        auth_service.change_password(user, "wrong-old", "newpass1")
    with pytest.raises(ValueError):
        auth_service.change_password(user, "123456", "123")  # 太短
    with pytest.raises(ValueError):
        auth_service.change_password(user, "123456", "123456")  # 与旧密码相同

    auth_service.change_password(user, "123456", "newpass1")
    assert user.check_password("newpass1")
    assert user.must_change_password is False  # 改密后清除强制改密标记


def test_admin_reset_password(app):
    from app.services import auth_service

    user = auth_service.create_user("erin", "erin@example.com")
    auth_service.change_password(user, "123456", "custompass")

    reset_user = auth_service.admin_reset_password(user.id)

    assert reset_user.check_password(auth_service.INITIAL_PASSWORD)
    assert reset_user.must_change_password is True

    with pytest.raises(ValueError):
        auth_service.admin_reset_password(99999)


def test_ensure_tables_seeds_admin_idempotent(app):
    from app.models import User
    from app.services import auth_service

    auth_service.ensure_tables()
    auth_service.ensure_tables()  # 幂等：重复调用不产生重复 admin

    admins = User.query.filter_by(username="admin").all()
    assert len(admins) == 1
    admin = admins[0]
    assert admin.is_admin is True
    # 默认密码 admin，首次登录强制改密
    assert admin.check_password(auth_service.DEFAULT_ADMIN_PASSWORD)
    assert admin.must_change_password is True
