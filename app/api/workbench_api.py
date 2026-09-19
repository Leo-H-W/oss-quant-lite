"""人机协同工作台 API：雷达 / 决策卡 / 审批 / 审计 / P0 约束。"""
from flask import Blueprint, jsonify, request
from loguru import logger

from app.services import workbench_service

workbench_bp = Blueprint("workbench", __name__)


def _ok(data, message="成功"):
    return jsonify({"code": 200, "message": message, "data": data})


def _err(message, http=400):
    return jsonify({"code": http, "message": message, "data": None}), http


@workbench_bp.route("/radar", methods=["GET"])
def radar():
    """股票池雷达统计（真实行情）。"""
    try:
        return _ok(workbench_service.get_radar())
    except Exception as e:  # noqa: BLE001
        logger.error(f"工作台雷达错误: {e}")
        return _err(f"服务器错误: {e}", 500)


@workbench_bp.route("/cards", methods=["GET"])
def cards():
    """决策卡列表（首次访问自动播种演示卡）。"""
    try:
        status = request.args.get("status") or None
        return _ok({"cards": workbench_service.list_cards(status=status)})
    except Exception as e:  # noqa: BLE001
        logger.error(f"决策卡列表错误: {e}")
        return _err(f"服务器错误: {e}", 500)


@workbench_bp.route("/cards", methods=["POST"])
def create_card():
    try:
        card = workbench_service.create_card(request.get_json(force=True) or {})
        return _ok(card, "决策卡已创建")
    except ValueError as e:
        return _err(str(e))
    except Exception as e:  # noqa: BLE001
        logger.error(f"创建决策卡错误: {e}")
        return _err(f"服务器错误: {e}", 500)


@workbench_bp.route("/cards/<int:card_id>/decide", methods=["POST"])
def decide(card_id):
    """人工审批：approve / reject / modify，全部留痕。"""
    try:
        card, error = workbench_service.decide_card(card_id, request.get_json(force=True) or {})
        if error:
            return _err(error, 422)
        return _ok(card, "审批完成，已留痕")
    except Exception as e:  # noqa: BLE001
        logger.error(f"审批错误: {e}")
        return _err(f"服务器错误: {e}", 500)


@workbench_bp.route("/audit", methods=["GET"])
def audit():
    try:
        card_no = request.args.get("card_no") or None
        limit = min(int(request.args.get("limit", 50)), 200)
        return _ok({"records": workbench_service.list_audit(card_no=card_no, limit=limit)})
    except Exception as e:  # noqa: BLE001
        logger.error(f"审计查询错误: {e}")
        return _err(f"服务器错误: {e}", 500)


@workbench_bp.route("/constraints", methods=["GET"])
def constraints():
    """P0 硬约束与当前已批准仓位分布。"""
    try:
        return _ok(workbench_service.get_constraints())
    except Exception as e:  # noqa: BLE001
        logger.error(f"约束查询错误: {e}")
        return _err(f"服务器错误: {e}", 500)
