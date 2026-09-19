"""人机协同工作台：决策卡与审批留痕模型。"""
from datetime import datetime

from app.extensions import db


class WbDecisionCard(db.Model):
    """AI 生成的交易决策卡，人工审批后状态流转，全部留痕。"""

    __tablename__ = "wb_decision_cards"

    id = db.Column(db.Integer, primary_key=True)
    card_no = db.Column(db.String(32), unique=True, nullable=False, index=True)
    ts_code = db.Column(db.String(16), nullable=False, index=True)
    name = db.Column(db.String(32), nullable=False, default="")
    direction = db.Column(db.String(8), nullable=False, default="buy")  # buy / sell / reduce
    strategy_tag = db.Column(db.String(16), nullable=False, default="")  # V防御 / G动量 / D左侧
    suggested_weight = db.Column(db.Float, nullable=False, default=0.0)  # 建议仓位（占组合 %）
    evidence_json = db.Column(db.Text, nullable=False, default="{}")  # AI 证据快照（JSON 字符串）
    ai_confidence = db.Column(db.Float, nullable=False, default=0.5)
    status = db.Column(db.String(16), nullable=False, default="pending", index=True)  # pending/approved/rejected/modified
    reason = db.Column(db.Text, nullable=False, default="")
    signer = db.Column(db.String(64), nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    decided_at = db.Column(db.DateTime)

    def to_dict(self):
        return {
            "id": self.id,
            "card_no": self.card_no,
            "ts_code": self.ts_code,
            "name": self.name,
            "direction": self.direction,
            "strategy_tag": self.strategy_tag,
            "suggested_weight": self.suggested_weight,
            "evidence": self.evidence_json,
            "ai_confidence": self.ai_confidence,
            "status": self.status,
            "reason": self.reason,
            "signer": self.signer,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "decided_at": self.decided_at.isoformat() if self.decided_at else None,
        }


class WbAuditTrail(db.Model):
    """审批操作审计轨迹（不可篡改，仅追加）。"""

    __tablename__ = "wb_audit_trail"

    id = db.Column(db.Integer, primary_key=True)
    card_no = db.Column(db.String(32), nullable=False, index=True)
    action = db.Column(db.String(16), nullable=False)  # create/approve/reject/modify
    detail_json = db.Column(db.Text, nullable=False, default="{}")
    operator = db.Column(db.String(64), nullable=False, default="system")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "card_no": self.card_no,
            "action": self.action,
            "detail": self.detail_json,
            "operator": self.operator,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
