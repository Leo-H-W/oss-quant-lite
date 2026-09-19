# -*- coding: utf-8 -*-
"""智能查数「常用模板」内置定义。

只读 SELECT，由 /api/text2sql/run-sql 经 QueryExecutor 统一执行
（触发 Parquet → SQLite 虚拟表加载，数据随每日同步自动刷新）。
"""
from typing import Dict, Any, Optional

BUILTIN_TEMPLATES: Dict[str, Dict[str, Any]] = {
    'ma_bullish': {
        'name': '均线多头排列（ma5>ma10>ma20）',
        'category': '均线',
        'description': '短期均线在上、中期均线在下的多头格局',
        'sql': (
            'SELECT m.ts_code, b.stock_name, b.daily_close, '
            'm.ma5, m.ma10, m.ma20 '
            'FROM stock_ma_data m JOIN stock_business b ON m.ts_code = b.ts_code '
            'WHERE m.ma5 > m.ma10 AND m.ma10 > m.ma20 '
            'ORDER BY m.ma5 / m.ma20 DESC'
        ),
    },
    'ma_above_20': {
        'name': '股价站上20日均线',
        'category': '均线',
        'description': '收盘价高于20日均线，偏强信号',
        'sql': (
            'SELECT m.ts_code, b.stock_name, b.daily_close, m.ma20, '
            'ROUND((b.daily_close - m.ma20) / m.ma20 * 100, 2) AS pct_above_ma20 '
            'FROM stock_ma_data m JOIN stock_business b ON m.ts_code = b.ts_code '
            'WHERE b.daily_close > m.ma20 '
            'ORDER BY pct_above_ma20 DESC'
        ),
    },
    'macd_golden_cross': {
        'name': 'MACD 金叉（DIF 上穿 DEA）',
        'category': '技术指标',
        'description': 'DIF 位于 DEA 上方，MACD 柱为正',
        'sql': (
            'SELECT f.ts_code, b.stock_name, f.macd_dif, f.macd_dea, f.macd '
            'FROM stock_factor f JOIN stock_business b ON f.ts_code = b.ts_code '
            'WHERE f.macd_dif > f.macd_dea '
            'ORDER BY f.macd DESC'
        ),
    },
    'rsi_oversold': {
        'name': 'RSI6 超卖（<30）',
        'category': '技术指标',
        'description': 'RSI6 低于 30，短期超卖',
        'sql': (
            'SELECT f.ts_code, b.stock_name, f.rsi_6, f.rsi_12, b.daily_close '
            'FROM stock_factor f JOIN stock_business b ON f.ts_code = b.ts_code '
            'WHERE f.rsi_6 < 30 '
            'ORDER BY f.rsi_6 ASC'
        ),
    },
    'kdj_golden_cross': {
        'name': 'KDJ 金叉（K 上穿 D）',
        'category': '技术指标',
        'description': 'K 值位于 D 值上方',
        'sql': (
            'SELECT f.ts_code, b.stock_name, f.kdj_k, f.kdj_d, f.kdj_j '
            'FROM stock_factor f JOIN stock_business b ON f.ts_code = b.ts_code '
            'WHERE f.kdj_k > f.kdj_d '
            'ORDER BY f.kdj_j DESC'
        ),
    },
    'pct_change_leaders': {
        'name': '涨幅榜',
        'category': '行情',
        'description': '最新交易日涨跌幅排名',
        'sql': (
            'SELECT ts_code, stock_name, daily_close, factor_pct_change, turnover_rate '
            'FROM stock_business ORDER BY factor_pct_change DESC'
        ),
    },
    'turnover_ranking': {
        'name': '换手率排行',
        'category': '行情',
        'description': '按换手率从高到低排名',
        'sql': (
            'SELECT ts_code, stock_name, turnover_rate, vol, daily_close '
            'FROM stock_business ORDER BY turnover_rate DESC'
        ),
    },
    'valuation_low_pe': {
        'name': '低估值榜（PE 升序）',
        'category': '估值',
        'description': 'PE(TTM) 从低到高，越小越便宜',
        'sql': (
            'SELECT ts_code, stock_name, pe_ttm, pb, daily_close '
            'FROM stock_business ORDER BY pe_ttm ASC'
        ),
    },
}


def get_builtin_template(template_id: str) -> Optional[Dict[str, Any]]:
    return BUILTIN_TEMPLATES.get(template_id)


def list_builtin_templates() -> Dict[str, Dict[str, Any]]:
    return BUILTIN_TEMPLATES
