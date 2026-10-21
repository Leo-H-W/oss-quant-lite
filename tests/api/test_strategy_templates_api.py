# -*- coding: utf-8 -*-
"""策略模板 API 合约测试。"""
import pytest


pytestmark = pytest.mark.module_backtest


def test_strategy_templates_list_api(app):
    client = app.test_client()

    response = client.get("/api/ml-factor/strategy/templates")

    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    assert "money_trace_tracking" in data["templates"]


def test_strategy_template_detail_api(app):
    client = app.test_client()

    response = client.get("/api/ml-factor/strategy/templates/money_trace_tracking")

    assert response.status_code == 200
    data = response.get_json()
    assert data["success"] is True
    config = data["template"]["config"]
    assert config["selection_method"] == "factor_based"
    assert set(config["weights"].keys()) == set(config["factor_list"])


def test_strategy_template_detail_api_unknown_id_returns_404(app):
    client = app.test_client()

    response = client.get("/api/ml-factor/strategy/templates/not_a_template")

    assert response.status_code == 404
