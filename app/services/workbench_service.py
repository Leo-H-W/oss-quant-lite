"""人机协同工作台服务：雷达统计、决策卡 CRUD、P0 约束校验、审计留痕。

数据口径：
- 行情统计（收盘价/涨跌幅/HV20/动量/估值）全部来自本地 Parquet（Baostock 同步），
  属于真实市场数据；
- 决策卡的 AI 置信度与证据文案为演示口径，界面已标注。
"""
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd

from app.extensions import db
from app.models.workbench import WbAuditTrail, WbDecisionCard

# 工作台默认股票池（可通过环境变量 WB_POOL 覆盖，逗号分隔 ts_code）
DEFAULT_POOL = [
    "600900.SH", "600519.SH", "600036.SH",
    "300750.SZ", "300308.SZ", "002594.SZ",
    "601899.SH", "688981.SH", "002475.SZ",
]

# P0 硬约束（A股股票口径，不可豁免；覆盖需双人联签）
P0_RULES = [
    {"rule": "单票占比 >12%", "action": "拦截", "level": "red"},
    {"rule": "单一行业 >40%", "action": "拦截", "level": "red"},
    {"rule": "组合回撤 ≥2.6% / ≥5%", "action": "新仓减半 / 仓位降至25%", "level": "orange"},
    {"rule": "日内单笔亏损 >0.5% / 单日 >1%", "action": "当日停手", "level": "red"},
    {"rule": "ST/*ST/退市整理/停牌", "action": "禁止买入", "level": "red"},
    {"rule": "涨停不追 / 跌停", "action": "不追买 / 强制复核", "level": "orange"},
    {"rule": "T+1 当日买入", "action": "不可卖出", "level": "orange"},
    {"rule": "日均成交额 <1亿", "action": "仓位限制 ≤1%", "level": "red"},
]

MAX_SINGLE_WEIGHT = 12.0
TRADING_DAYS_PER_YEAR = 244


def _data_dir():
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..", "data")


def _pool():
    raw = os.getenv("WB_POOL", "")
    if raw.strip():
        return [c.strip() for c in raw.split(",") if c.strip()]
    return list(DEFAULT_POOL)


def _read_parquet_safe(path):
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def get_radar():
    """计算股票池雷达统计（真实行情，截至最新交易日）。"""
    data_dir = _data_dir()
    daily = _read_parquet_safe(os.path.join(data_dir, "daily_history", "daily"))
    basic_df = _read_parquet_safe(os.path.join(data_dir, "daily_basic", "daily"))
    stock_basic = _read_parquet_safe(os.path.join(data_dir, "stock_basic.parquet"))

    name_map, industry_map = {}, {}
    if stock_basic is not None and not stock_basic.empty:
        name_map = dict(zip(stock_basic["ts_code"], stock_basic.get("name", "")))
        industry_map = dict(zip(stock_basic["ts_code"], stock_basic.get("industry", "")))

    items = []
    if daily is None or daily.empty:
        return {"as_of": None, "items": items}

    pool = _pool()
    daily = daily[daily["ts_code"].isin(pool)]
    as_of = str(daily["trade_date"].max())

    # 估值：每个标的最新一期
    val_map = {}
    if basic_df is not None and not basic_df.empty:
        b = basic_df.sort_values("trade_date").groupby("ts_code").tail(1)
        for _, r in b.iterrows():
            val_map[r["ts_code"]] = {"pe_ttm": r.get("pe_ttm"), "pb": r.get("pb"),
                                     "turnover_rate": r.get("turnover_rate")}

    for ts_code, g in daily.groupby("ts_code"):
        g = g.sort_values("trade_date")
        closes = g["close"].astype(float)
        last = g.iloc[-1]
        ma20 = float(closes.tail(20).mean()) if len(closes) >= 20 else float(closes.mean())
        rets = np.log(closes / closes.shift(1)).dropna()
        hv20 = float(rets.tail(20).std() * np.sqrt(TRADING_DAYS_PER_YEAR) * 100) if len(rets) >= 2 else None
        mom20 = float((closes.iloc[-1] / closes.iloc[-21] - 1) * 100) if len(closes) >= 21 else None
        ma20_gap = float((closes.iloc[-1] / ma20 - 1) * 100) if ma20 else None
        amount_yi = float(last.get("amount", 0) or 0) / 1e8

        pct = float(last.get("pct_chg") or 0)
        if pct >= 2 and (hv20 or 0) >= 25:
            signal = "hot"
        elif pct >= 1.5:
            signal = "hot"
        elif (mom20 or 0) >= 1 and (hv20 or 99) <= 18:
            signal = "good"
        elif (mom20 or 0) <= -15:
            signal = "oversold"
        elif pct <= -0.5:
            signal = "watch"
        else:
            signal = "flat"

        items.append({
            "ts_code": ts_code,
            "name": name_map.get(ts_code, ""),
            "industry": str(industry_map.get(ts_code, "") or ""),
            "close": float(last["close"]),
            "pct_chg": round(pct, 2),
            "ma20_gap": round(ma20_gap, 2) if ma20_gap is not None else None,
            "hv20": round(hv20, 1) if hv20 is not None else None,
            "mom20": round(mom20, 2) if mom20 is not None else None,
            "amount_yi": round(amount_yi, 2),
            "pe_ttm": val_map.get(ts_code, {}).get("pe_ttm"),
            "pb": val_map.get(ts_code, {}).get("pb"),
            "signal": signal,
        })

    order = {"hot": 0, "watch": 1, "oversold": 2, "good": 3, "flat": 4}
    items.sort(key=lambda x: (order.get(x["signal"], 9), -(x["pct_chg"] or 0)))
    return {"as_of": as_of, "items": items}


def _snapshot_evidence(ts_code):
    """从雷达统计生成决策卡证据快照（真实行情字段）。"""
    radar = get_radar()
    item = next((i for i in radar["items"] if i["ts_code"] == ts_code), None)
    if item is None:
        return {}
    return {
        "as_of": radar["as_of"],
        "close": item["close"],
        "pct_chg": item["pct_chg"],
        "hv20": item["hv20"],
        "mom20": item["mom20"],
        "ma20_gap": item["ma20_gap"],
        "pe_ttm": item["pe_ttm"],
        "pb": item["pb"],
        "note": "行情字段为 Baostock 真实数据；AI 置信度与策略打分为演示口径",
    }


def _audit(card_no, action, detail, operator="system"):
    db.session.add(WbAuditTrail(
        card_no=card_no, action=action,
        detail_json=json.dumps(detail, ensure_ascii=False),
        operator=operator,
    ))


def seed_demo_cards():
    """空表时生成两张演示决策卡（基于最新真实行情快照）。"""
    if WbDecisionCard.query.count() > 0:
        return
    radar = get_radar()
    by_code = {i["ts_code"]: i for i in radar["items"]}

    def mk(card_no, ts_code, direction, tag, weight, conf, extra_reason):
        item = by_code.get(ts_code, {})
        ev = _snapshot_evidence(ts_code)
        ev["reasoning"] = extra_reason
        card = WbDecisionCard(
            card_no=card_no, ts_code=ts_code,
            name=item.get("name", ""), direction=direction,
            strategy_tag=tag, suggested_weight=weight,
            evidence_json=json.dumps(ev, ensure_ascii=False),
            ai_confidence=conf, status="pending",
        )
        db.session.add(card)
        db.session.flush()
        _audit(card_no, "create", {"source": "ai-agent", "evidence": ev})

    mk("DC-PENDING-001", "600900.SH", "buy", "V防御", 3.0, 0.78,
       "低波(HV20<15%)+贴合MA20+盈利预期稳定，防御加仓候选")
    mk("DC-PENDING-002", "002475.SZ", "buy", "G动量", 2.0, 0.64,
       "放量突破+主力净流入（演示口径），右侧动量候选，仓位减半试错")
    db.session.commit()


def _ensure_tables():
    """工作台表随首次请求惰性创建（与项目既有 SQLite 初始化风格一致）。"""
    db.create_all()


def list_cards(status=None):
    _ensure_tables()
    seed_demo_cards()
    q = WbDecisionCard.query.order_by(WbDecisionCard.created_at.desc())
    if status:
        q = q.filter_by(status=status)
    return [c.to_dict() for c in q.all()]


def create_card(payload):
    _ensure_tables()
    ts_code = payload.get("ts_code", "").strip()
    if not ts_code:
        raise ValueError("ts_code 必填")
    weight = float(payload.get("suggested_weight") or 0)
    if weight <= 0 or weight > MAX_SINGLE_WEIGHT:
        raise ValueError(f"建议仓位须在 0-{MAX_SINGLE_WEIGHT}% 之间")
    radar = get_radar()
    item = next((i for i in radar["items"] if i["ts_code"] == ts_code), {})
    card_no = "DC-" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    ev = _snapshot_evidence(ts_code)
    ev["reasoning"] = payload.get("reasoning", "")
    card = WbDecisionCard(
        card_no=card_no, ts_code=ts_code, name=item.get("name", ""),
        direction=payload.get("direction", "buy"),
        strategy_tag=payload.get("strategy_tag", ""),
        suggested_weight=weight,
        evidence_json=json.dumps(ev, ensure_ascii=False),
        ai_confidence=float(payload.get("ai_confidence") or 0.5),
        status="pending",
    )
    db.session.add(card)
    db.session.flush()
    _audit(card_no, "create", {"payload": payload})
    db.session.commit()
    return card.to_dict()


def _approved_weight(ts_code, exclude_id=None):
    q = WbDecisionCard.query.filter_by(ts_code=ts_code, status="approved")
    if exclude_id:
        q = q.filter(WbDecisionCard.id != exclude_id)
    return float(sum(c.suggested_weight for c in q.all()))


def decide_card(card_id, payload):
    """人工审批：P0 校验 → 状态流转 → 留痕。返回 (card, error)。"""
    _ensure_tables()
    card = WbDecisionCard.query.get(card_id)
    if card is None:
        return None, "决策卡不存在"
    if card.status != "pending":
        return None, f"当前状态 {card.status} 不可重复审批"

    action = payload.get("action", "")
    reason = (payload.get("reason") or "").strip()
    if action not in ("approve", "reject", "modify"):
        return None, "action 必须是 approve / reject / modify"
    if not reason:
        return None, "必须填写审批理由（留痕要求）"

    operator = (payload.get("signer") or "operator").strip() or "operator"
    new_weight = card.suggested_weight
    if action == "modify":
        new_weight = float(payload.get("suggested_weight") or card.suggested_weight)
        if new_weight <= 0 or new_weight > MAX_SINGLE_WEIGHT:
            return None, f"修改后仓位须在 0-{MAX_SINGLE_WEIGHT}% 之间"

    # P0 校验：单票合计上限（ approve / modify 都检查）
    if action in ("approve", "modify"):
        total = _approved_weight(card.ts_code, exclude_id=card.id) + new_weight
        if total > MAX_SINGLE_WEIGHT:
            return None, (
                f"P0 拦截：{card.ts_code} 批准后合计仓位 {total:.1f}% "
                f"超过单票上限 {MAX_SINGLE_WEIGHT}%（现有已批准 {_approved_weight(card.ts_code):.1f}%）"
            )

    card.status = "approved" if action == "approve" else ("rejected" if action == "reject" else "modified")
    card.reason = reason
    card.signer = operator
    card.suggested_weight = new_weight
    card.decided_at = datetime.utcnow()
    _audit(card.card_no, action, {
        "reason": reason, "weight": new_weight, "operator": operator,
    }, operator=operator)
    db.session.commit()
    return card.to_dict(), None


def list_audit(card_no=None, limit=50):
    q = WbAuditTrail.query.order_by(WbAuditTrail.created_at.desc()).limit(limit)
    if card_no:
        q = WbAuditTrail.query.filter_by(card_no=card_no).order_by(
            WbAuditTrail.created_at.desc()).limit(limit)
    return [a.to_dict() for a in q.all()]


def get_constraints():
    """P0 规则 + 当前已批准仓位分布。"""
    _ensure_tables()
    exposure = {}
    for c in WbDecisionCard.query.filter_by(status="approved").all():
        exposure[c.ts_code] = round(exposure.get(c.ts_code, 0) + c.suggested_weight, 2)
    return {
        "max_single_weight": MAX_SINGLE_WEIGHT,
        "rules": P0_RULES,
        "approved_exposure": exposure,
        "approved_total": round(sum(exposure.values()), 2),
    }
