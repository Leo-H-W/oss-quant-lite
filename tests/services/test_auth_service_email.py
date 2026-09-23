"""重置邮件发送合约：SMTP 未配置时降级到日志，配置齐全时走 smtplib。"""

from unittest.mock import patch

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
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{tmp_path}/svc_email.db",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()


def test_degrades_to_log_when_smtp_unconfigured(app):
    from app.services import auth_service

    user = auth_service.create_user("judy", "judy@example.com")
    app.config["EMAIL_USERNAME"] = ""
    app.config["EMAIL_PASSWORD"] = ""

    with patch("app.services.auth_service.logger") as mock_logger:
        sent = auth_service.send_reset_email(user, "http://localhost:9090/reset-password/tok123")

    assert sent is False
    # 降级：重置链接必须出现在日志里，供管理员人工转告
    logged = " ".join(str(call) for call in mock_logger.warning.call_args_list)
    assert "tok123" in logged


def test_sends_via_smtp_when_configured(app):
    from app.services import auth_service

    user = auth_service.create_user("karl", "karl@example.com")
    app.config.update(
        EMAIL_SMTP_SERVER="smtp.example.com",
        EMAIL_SMTP_PORT=587,
        EMAIL_USERNAME="sender@example.com",
        EMAIL_PASSWORD="secret",
        EMAIL_FROM="",  # 为空时回退 EMAIL_USERNAME
    )

    with patch("app.services.auth_service.smtplib.SMTP") as mock_smtp:
        sent = auth_service.send_reset_email(user, "http://localhost:9090/reset-password/tok456")

    assert sent is True
    smtp = mock_smtp.return_value.__enter__.return_value
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("sender@example.com", "secret")
    smtp.sendmail.assert_called_once()
    from_addr, to_addrs, _body = smtp.sendmail.call_args[0]
    assert from_addr == "sender@example.com"
    assert to_addrs == ["karl@example.com"]


def test_smtp_failure_degrades_to_log(app):
    from app.services import auth_service

    user = auth_service.create_user("liam", "liam@example.com")
    app.config.update(
        EMAIL_SMTP_SERVER="smtp.example.com",
        EMAIL_SMTP_PORT=587,
        EMAIL_USERNAME="sender@example.com",
        EMAIL_PASSWORD="secret",
    )

    with patch("app.services.auth_service.smtplib.SMTP", side_effect=OSError("connection refused")), \
         patch("app.services.auth_service.logger") as mock_logger:
        sent = auth_service.send_reset_email(user, "http://localhost:9090/reset-password/tok789")

    assert sent is False
    logged = " ".join(str(call) for call in mock_logger.warning.call_args_list)
    assert "tok789" in logged
