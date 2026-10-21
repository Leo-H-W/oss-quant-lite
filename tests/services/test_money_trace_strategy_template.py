# -*- coding: utf-8 -*-
"""「资金痕迹跟踪」策略模板合约测试。

模板是一个可直接传给 BacktestEngine.run_backtest 的 strategy_config
（start_date/end_date 由调用方补），其 factor_list 必须全部指向
FactorEngine 已注册的内置因子，权重合法。
"""
import pytest

from app.services.factor_engine import FactorEngine
from app.services.strategy_templates import (
    get_strategy_template,
    list_strategy_templates,
)

pytestmark = pytest.mark.module_backtest

TEMPLATE_ID = "money_trace_tracking"


def test_template_registered_and_listable():
    templates = list_strategy_templates()
    assert TEMPLATE_ID in templates
    assert templates[TEMPLATE_ID]["name"]


def test_template_required_config_keys():
    config = get_strategy_template(TEMPLATE_ID)["config"]
    for key in ("selection_method", "factor_list", "weights", "top_n"):
        assert key in config, f"策略模板缺少必需配置项 {key}"
    assert config["selection_method"] == "factor_based"
    assert isinstance(config["top_n"], int) and config["top_n"] > 0
    assert config.get("rebalance_frequency", "weekly") in ("daily", "weekly", "monthly")


def test_template_factor_list_all_registered_builtin():
    engine = FactorEngine()
    config = get_strategy_template(TEMPLATE_ID)["config"]
    for factor_id in config["factor_list"]:
        assert factor_id in engine.builtin_factors, (
            f"模板引用的因子 {factor_id} 未在 FactorEngine 注册"
        )


def test_template_weights_cover_factor_list_and_sum_to_one():
    config = get_strategy_template(TEMPLATE_ID)["config"]
    weights = config["weights"]
    assert set(weights.keys()) == set(config["factor_list"])
    assert all(w > 0 for w in weights.values())
    assert abs(sum(weights.values()) - 1.0) < 1e-9
