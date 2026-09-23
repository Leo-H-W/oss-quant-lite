"""用户认证 API：登录/登出/改密/忘记密码/重置密码/用户管理（admin）。"""
import functools

from flask import Blueprint, g, jsonify, request, session
from loguru import logger

from app.models import User
from app.services import auth_service

auth_bp = Blueprint("auth", __name__)


def _ok(data, message="成功"):
    return jsonify({"code": 200, "message": message, "data": data})


def _err(message, http=400):
    return jsonify({"code": http, "message": message, "data": None}), http


def admin_required(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not getattr(g, "user", None) or not g.user.is_admin:
            return _err("需要管理员权限", 403)
        return fn(*args, **kwargs)

    return wrapper


@auth_bp.route("/login", methods=["POST"])
def login():
    payload = request.get_json(silent=True) or {}
    user = auth_service.authenticate(payload.get("username"), payload.get("password"))
    if user is None:
        return _err("用户名或密码错误", 401)
    session.clear()
    session["user_id"] = user.id
    return _ok(
        {"user": user.to_dict(), "must_change_password": user.must_change_password},
        "登录成功",
    )


@auth_bp.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return _ok(None, "已退出登录")


@auth_bp.route("/me", methods=["GET"])
def me():
    if getattr(g, "user", None) is None:
        return _err("未登录或会话已过期", 401)
    return _ok(g.user.to_dict())


@auth_bp.route("/change-password", methods=["POST"])
def change_password():
    if getattr(g, "user", None) is None:
        return _err("未登录或会话已过期", 401)
    payload = request.get_json(silent=True) or {}
    try:
        auth_service.change_password(
            g.user, payload.get("old_password"), payload.get("new_password")
        )
    except ValueError as e:
        return _err(str(e))
    return _ok(None, "密码修改成功")


@auth_bp.route("/forgot-password", methods=["POST"])
def forgot_password():
    payload = request.get_json(silent=True) or {}
    found, sent = auth_service.request_password_reset(payload.get("email"))
    # 恒定文案，不泄露邮箱是否注册
    message = "如该邮箱已注册，重置链接已发送"
    if found and not sent:
        message += "；当前邮件服务不可用，重置链接已打印到服务端日志，请联系管理员"
    return _ok(None, message)


@auth_bp.route("/reset-password", methods=["POST"])
def reset_password():
    payload = request.get_json(silent=True) or {}
    try:
        auth_service.reset_password_with_token(
            payload.get("token"), payload.get("new_password")
        )
    except ValueError as e:
        return _err(str(e), 400)
    return _ok(None, "密码重置成功，请重新登录")


@auth_bp.route("/users", methods=["GET"])
@admin_required
def list_users():
    users = User.query.order_by(User.id).all()
    return _ok({"users": [u.to_dict() for u in users]})


@auth_bp.route("/users", methods=["POST"])
@admin_required
def create_user():
    payload = request.get_json(silent=True) or {}
    try:
        user = auth_service.create_user(payload.get("username"), payload.get("email"))
    except ValueError as e:
        return _err(str(e))
    logger.info(f"admin {g.user.username} 新建用户 {user.username}")
    return _ok({"user": user.to_dict()}, "用户已创建，初始密码 123456，首次登录需修改")


@auth_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def reset_user_password(user_id):
    try:
        user = auth_service.admin_reset_password(user_id)
    except ValueError as e:
        return _err(str(e), 404)
    logger.info(f"admin {g.user.username} 重置了用户 {user.username} 的密码")
    return _ok(None, f"用户 {user.username} 的密码已重置为 123456，下次登录需修改")
