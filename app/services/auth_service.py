"""用户认证服务：用户管理、密码、重置 token、重置邮件。

设计要点：
- 惰性建表 + admin 种子按数据库 URL 缓存（同一进程可挂多个 app/库，测试隔离需要）；
- 重置 token 用 itsdangerous 签名，payload 带密码哈希指纹——改密后旧链接自动失效，
  无需 token 表即获得一次性语义；
- SMTP 未配置（EMAIL_USERNAME/EMAIL_PASSWORD 为空）或发送失败时降级：
  重置链接打印到服务端日志，由管理员人工转告。
"""
import hashlib
import smtplib
import threading
from email.header import Header
from email.mime.text import MIMEText

from flask import current_app, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from loguru import logger
from sqlalchemy.exc import IntegrityError
from werkzeug.security import check_password_hash, generate_password_hash

from app.extensions import db
from app.models import User

INITIAL_PASSWORD = "123456"
DEFAULT_ADMIN_PASSWORD = "admin"
RESET_TOKEN_SALT = "password-reset-v1"

# 对不存在的用户也跑一次哈希校验，抹平响应时间差，防用户名枚举
_DUMMY_HASH = generate_password_hash("dummy")

_seeded_dbs = set()
_seeded_lock = threading.Lock()


def ensure_tables():
    """惰性建表 + 种子 admin（幂等，按数据库 URL 去重）。"""
    key = str(db.engine.url)
    if key in _seeded_dbs:
        return
    with _seeded_lock:
        if key in _seeded_dbs:
            return
        db.create_all()
        _seed_admin()
        _seeded_dbs.add(key)


def _seed_admin():
    if User.query.filter_by(username="admin").first():
        return
    admin = User(
        username="admin", email="", is_admin=True, must_change_password=True
    )
    admin.set_password(DEFAULT_ADMIN_PASSWORD)
    db.session.add(admin)
    db.session.commit()
    logger.warning("已创建默认管理员账号 admin/admin，首次登录将强制修改密码")


def get_user(user_id):
    try:
        return db.session.get(User, int(user_id))
    except (TypeError, ValueError):
        return None


def authenticate(username, password):
    """校验用户名密码，失败统一返回 None（调用方报统一文案）。"""
    username = (username or "").strip()
    if not username or not password:
        return None
    user = User.query.filter_by(username=username).first()
    if user is None:
        check_password_hash(_DUMMY_HASH, password)
        return None
    if not user.check_password(password):
        return None
    return user


def _validate_new_password(new_password, old_password=None):
    if not new_password or len(new_password) < 6:
        raise ValueError("新密码长度至少 6 位")
    if old_password is not None and new_password == old_password:
        raise ValueError("新密码不能与原密码相同")


def change_password(user, old_password, new_password):
    if not user.check_password(old_password or ""):
        raise ValueError("原密码错误")
    _validate_new_password(new_password, old_password)
    user.set_password(new_password)
    user.must_change_password = False
    db.session.commit()


def create_user(username, email):
    """admin 新建用户：初始密码 123456，首次登录强制改密。"""
    username = (username or "").strip()
    email = (email or "").strip()
    if not username:
        raise ValueError("用户名不能为空")
    if not email:
        raise ValueError("邮箱不能为空")
    if User.query.filter_by(username=username).first():
        raise ValueError("用户名已存在")
    user = User(username=username, email=email, is_admin=False, must_change_password=True)
    user.set_password(INITIAL_PASSWORD)
    db.session.add(user)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        raise ValueError("用户名已存在")
    return user


def admin_reset_password(user_id):
    """admin 重置他人密码为 123456，并恢复首次登录强制改密。"""
    user = db.session.get(User, int(user_id))
    if user is None:
        raise ValueError("用户不存在")
    user.set_password(INITIAL_PASSWORD)
    user.must_change_password = True
    db.session.commit()
    return user


def _password_fingerprint(user):
    return hashlib.sha256(user.password_hash.encode()).hexdigest()[:16]


def _serializer():
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=RESET_TOKEN_SALT)


def generate_reset_token(user):
    return _serializer().dumps({"uid": user.id, "ph": _password_fingerprint(user)})


def verify_reset_token(token):
    """验证重置 token；无效/过期/改密后均返回 None。"""
    max_age = current_app.config.get("PASSWORD_RESET_TOKEN_MAX_AGE", 3600)
    try:
        data = _serializer().loads(token, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return None
    user = db.session.get(User, data.get("uid"))
    if user is None:
        return None
    if _password_fingerprint(user) != data.get("ph"):
        return None
    return user


def reset_password_with_token(token, new_password):
    """凭重置 token 设置新密码；token 无效或密码不合规抛 ValueError。"""
    user = verify_reset_token(token)
    if user is None:
        raise ValueError("重置链接无效或已过期")
    _validate_new_password(new_password)
    user.set_password(new_password)
    user.must_change_password = False
    db.session.commit()
    return user


def send_reset_email(user, reset_url):
    """发送重置邮件；SMTP 未配置或发送失败时降级到日志，返回 False。"""
    cfg = current_app.config
    username = cfg.get("EMAIL_USERNAME", "")
    password = cfg.get("EMAIL_PASSWORD", "")
    if not username or not password:
        logger.warning(f"[SMTP未配置] 用户 {user.username} 的密码重置链接: {reset_url}")
        return False
    msg = MIMEText(
        f"您好 {user.username}，\n\n"
        f"请点击以下链接重置密码（链接 1 小时内有效）：\n{reset_url}\n\n"
        "如非本人操作请忽略本邮件。",
        "plain",
        "utf-8",
    )
    msg["Subject"] = Header("QuantAnalysis 密码重置", "utf-8")
    msg["From"] = cfg.get("EMAIL_FROM") or username
    msg["To"] = user.email
    try:
        with smtplib.SMTP(
            cfg.get("EMAIL_SMTP_SERVER", "smtp.qq.com"),
            int(cfg.get("EMAIL_SMTP_PORT", 587)),
            timeout=10,
        ) as smtp:
            smtp.starttls()
            smtp.login(username, password)
            smtp.sendmail(msg["From"], [user.email], msg.as_string())
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(f"重置邮件发送失败（{e}），用户 {user.username} 的重置链接: {reset_url}")
        return False


def request_password_reset(email):
    """忘记密码入口：命中邮箱则发重置邮件。返回 (found, sent)，调用方恒定文案。"""
    email = (email or "").strip()
    if not email:
        return False, False
    user = User.query.filter_by(email=email).first()
    if user is None:
        return False, False
    reset_url = url_for(
        "auth_pages.reset_password", token=generate_reset_token(user), _external=True
    )
    return True, send_reset_email(user, reset_url)
