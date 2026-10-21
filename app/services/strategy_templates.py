# -*- coding: utf-8 -*-
"""内置策略模板：可直接作为 BacktestEngine.run_backtest 的 strategy_config
（start_date/end_date 由调用方补），或供前端回测表单预填。

与 text2sql 的 builtin_templates（SQL 模板）不同，这里模板产出的是
回测策略配置，factor_list 必须全部指向 FactorEngine 已注册的内置因子。
"""
from typing import Any, Dict, Optional

STRATEGY_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "money_trace_tracking": {
        "name": "资金痕迹跟踪",
        "category": "资金流向",
        "description": (
            "复合资金痕迹因子选股：CMF(20日)、MFI(14日) 刻画量价资金流，"
            "主力净流入率(5日/20日) 跟踪大单动向（华泰《单因子测试之资金流向因子》口径），"
            "聪明钱Q 识别分钟级主力成本（无分钟数据时该因子自动缺席）。"
            "因子复合打分取 top_n，周频调仓。"
        ),
        "config": {
            "selection_method": "factor_based",
            "factor_list": [
                "cmf_20d",
                "mfi_14d",
                "main_net_inflow_rate_5d",
                "main_net_inflow_rate_20d",
                "smart_money_q",
            ],
            "weights": {
                "cmf_20d": 0.2,
                "mfi_14d": 0.2,
                "main_net_inflow_rate_5d": 0.25,
                "main_net_inflow_rate_20d": 0.2,
                "smart_money_q": 0.15,
            },
            "top_n": 30,
            "rebalance_frequency": "weekly",
            "optimization": {"method": "equal_weight"},
        },
    },
}


def list_strategy_templates() -> Dict[str, Dict[str, Any]]:
    return STRATEGY_TEMPLATES


def get_strategy_template(template_id: str) -> Optional[Dict[str, Any]]:
    return STRATEGY_TEMPLATES.get(template_id)
