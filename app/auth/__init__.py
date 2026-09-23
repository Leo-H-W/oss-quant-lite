"""全站登录守卫 + 认证页面蓝图。

守卫约定：
- 请求期读取 AUTH_ENABLED（测试可关闭，既有合约测试不受影响）；
- 未登录：/api/* 返回 401 JSON，页面 302 到 /login?next=<path>；
- must_change_password 用户仅放行改密相关路径，其余页面 302 /change-password、API 403；
- session 只存 user_id，每请求加载到 g.user 供模板使用。
"""
from flask import (
    Blueprint, abort, current_app, g, jsonify, redirect, render_template, request,
    session, url_for,
)

auth_pages_bp = Blueprint("auth_pages", __name__)

# 未登录也可访问的精确路径
_EXEMPT_PATHS = {
    "/login",
    "/forgot-password",
    "/api/auth/login",
    "/api/auth/forgot-password",
    "/api/auth/reset-password",
}
# 未登录也可访问的路径前缀（静态资源、重置链接、websocket 握手）
_EXEMPT_PREFIXES = ("/static/", "/reset-password/", "/socket.io/", "/favicon")
# 已登录但必须改密时额外放行的路径
_MUST_CHANGE_ALLOWED = {
    "/change-password",
    "/api/auth/change-password",
    "/api/auth/logout",
    "/api/auth/me",
}


def _is_api(path):
    return path.startswith("/api/")


def _exempt(path):
    return path in _EXEMPT_PATHS or any(path.startswith(p) for p in _EXEMPT_PREFIXES)


def init_auth_guard(app):
    """注册全站 before_request 登录守卫。"""

    @app.before_request
    def _require_login():
        g.user = None
        if not current_app.config.get("AUTH_ENABLED", True):
            return None

        from app.services import auth_service

        auth_service.ensure_tables()

        uid = session.get("user_id")
        if uid is not None:
            g.user = auth_service.get_user(uid)
            if g.user is None:  # 用户已被删除，会话作废
                session.clear()

        path = request.path
        if g.user is None:
            if _exempt(path):
                return None
            if _is_api(path):
                return jsonify({"code": 401, "message": "未登录或会话已过期", "data": None}), 401
            return redirect(url_for("auth_pages.login", next=path))

        if g.user.must_change_password:
            if path in _MUST_CHANGE_ALLOWED or any(
                path.startswith(p) for p in _EXEMPT_PREFIXES
            ):
                return None
            if _is_api(path):
                return jsonify({
                    "code": 403,
                    "message": "请先修改初始密码",
                    "data": {"must_change_password": True},
                }), 403
            return redirect(url_for("auth_pages.change_password"))
        return None


@auth_pages_bp.route("/login")
def login():
    """登录页（独立页，不带主导航）。"""
    return render_template("auth/login.html")


@auth_pages_bp.route("/change-password")
def change_password():
    """修改密码页（守卫强制改密时的落地页）。"""
    return render_template("auth/change_password.html")


@auth_pages_bp.route("/forgot-password")
def forgot_password():
    """忘记密码页（独立页）。"""
    return render_template("auth/forgot_password.html")


@auth_pages_bp.route("/reset-password/<token>")
def reset_password(token):
    """重置密码页（独立页，token 渲染进页面供提交）。"""
    return render_template("auth/reset_password.html", token=token)


@auth_pages_bp.route("/users")
def user_management():
    """用户管理页（仅 admin）。"""
    if not getattr(g, "user", None) or not g.user.is_admin:
        abort(403)
    return render_template("auth/user_management.html")
