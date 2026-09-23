"""密码重置 token 合约：生成/验证、防篡改、过期、改密后失效。"""

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
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{tmp_path}/svc_tokens.db",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()


def test_token_roundtrip(app):
    from app.services import auth_service

    user = auth_service.create_user("frank", "frank@example.com")
    token = auth_service.generate_reset_token(user)

    verified = auth_service.verify_reset_token(token)
    assert verified is not None
    assert verified.id == user.id


def test_tampered_token_rejected(app):
    from app.services import auth_service

    user = auth_service.create_user("grace", "grace@example.com")
    token = auth_service.generate_reset_token(user)

    assert auth_service.verify_reset_token(token + "tampered") is None
    assert auth_service.verify_reset_token("not-a-token") is None


def test_expired_token_rejected(app):
    from app.services import auth_service

    user = auth_service.create_user("heidi", "heidi@example.com")
    token = auth_service.generate_reset_token(user)

    app.config["PASSWORD_RESET_TOKEN_MAX_AGE"] = -1  # 立即过期
    assert auth_service.verify_reset_token(token) is None


def test_token_invalidated_after_password_change(app):
    from app.services import auth_service

    user = auth_service.create_user("ivan", "ivan@example.com")
    token = auth_service.generate_reset_token(user)

    # 改密后旧 token 必须失效（一次性语义，无需 token 表）
    auth_service.change_password(user, "123456", "newpass1")
    assert auth_service.verify_reset_token(token) is None
