#!/usr/bin/env python3
"""按通达信趋势公式对单只 A 股进行日线回测。

用法：
    .venv/bin/python huice-func1.py 300454
    .venv/bin/python huice-func1.py help
"""

import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data"))).expanduser().resolve()
INITIAL_CAPITAL = 100000.0
COMMISSION_RATE = 0.0003
MIN_COMMISSION = 5.0
RISK_FREE_RATE = 0.03

HELP_TEXT = r"""huice-func1.py — 单股票趋势公式日线回测
========================================

用法
----
  .venv/bin/python huice-func1.py 300454
  .venv/bin/python huice-func1.py 300454.SZ
  .venv/bin/python huice-func1.py help

参数
----
  股票代码：6 位代码，或带 SH/SZ/BJ 后缀的完整代码。
            不带后缀时自动推断：6/9 → SH，0/2/3 → SZ，
            4/8/920 → BJ。
  help：显示本说明；-h、--help 或不传参数也可以显示。

计算公式
--------
  TS   = 当前股票已有 K 线数量 >= 30
  MM   = EMA((3*C + H + L + O) / 6, 21)
  FL   = HHV(MM, 5)，即包含当前日在内的 5 日 MM 最高值
  UP   = MM >= FL
  BUY  = UP 当日成立、前一日不成立，并且 TS 成立
  SELL = UP 当日不成立、前一日成立，并且 TS 成立

EMA 使用 adjust=False 的递推算法，初始值为第一个输入值，与常见
通达信 EMA 递推口径一致。FS、板块信息和资金流向仅为原公式的展示
内容，不产生交易信号，因此本脚本不将它们纳入回测。

回测原理与防止未来函数
----------------------
1. 读取 data/daily_history/daily 下的本地 Parquet 日线数据。
2. 根据每日 pre_close 链式重建后复权 OHLC：
       factor[t] = factor[t-1] * close[t-1] / pre_close[t]
       复权价格 = 原始价格 * factor
   这样跨分红、送配日的价格收益保持连续，不会把除权缺口误算为亏损。
3. 在第 t 日收盘后计算信号，在第 t+1 个有行情的交易日开盘执行，
   不使用当日尚未收盘时无法获得的数据。
4. 单票全仓：买入时尽量使用全部现金，卖出时清空全部持仓。
   最后仍持有的仓位按最后一日收盘价估值，不强制平仓。
5. 回测从第 30 根 K 线开始；此前的数据只用于指标预热。
   同期买入持有基准从同一日开始，不扣交易成本。

默认交易假设
------------
  初始资金：100,000 元
  佣金：买卖双边万分之三，每次最低 5 元
  印花税：仅卖出收取；2023-08-28 前为 0.1%，之后为 0.05%
  滑点：0
  不模拟涨跌停导致的无法成交，不限制 100 股整手交易
  夏普比率按 252 个交易日年化，无风险利率为 3%

输出指标
--------
  累计/年化收益、最大回撤、波动率、夏普比率、交易次数、胜率、
  盈亏比、平均持仓时间、仓位时间占比、交易成本、分年度收益、
  最近五次完整交易和最新信号。胜率按完整买卖回合统计。

环境
----
  默认数据目录为脚本所在目录下的 data；可用 DATA_DIR 环境变量覆盖。
  依赖 pandas、numpy、pyarrow，建议使用项目 .venv 中的 Python。

示例
----
  .venv/bin/python huice-func1.py 300454
  .venv/bin/python huice-func1.py 600519.SH
  DATA_DIR=/path/to/data .venv/bin/python huice-func1.py 000001
"""


def normalize_stock_code(value):
    """把用户输入规范为 Tushare 风格代码，例如 300454.SZ。"""
    text = value.strip().upper()
    match = re.fullmatch(r"(\d{6})(?:\.(SH|SZ|BJ))?", text)
    if not match:
        raise ValueError(
            "股票代码格式无效：{}；应为 6 位数字，可带 .SH/.SZ/.BJ 后缀".format(value)
        )
    symbol, exchange = match.groups()
    if exchange:
        return "{}.{}".format(symbol, exchange)
    if symbol.startswith("920") or symbol.startswith(("4", "8")):
        exchange = "BJ"
    elif symbol.startswith(("6", "9")):
        exchange = "SH"
    else:
        exchange = "SZ"
    return "{}.{}".format(symbol, exchange)


def load_daily(ts_code):
    """只读取目标股票的日线，利用 PyArrow 在扫描时过滤。"""
    daily_dir = DATA_DIR / "daily_history" / "daily"
    if not daily_dir.is_dir():
        raise FileNotFoundError("本地日线目录不存在：{}".format(daily_dir))
    dataset = ds.dataset(str(daily_dir), format="parquet", partitioning="hive")
    required = {"ts_code", "trade_date", "open", "high", "low", "close", "pre_close"}
    missing = required.difference(dataset.schema.names)
    if missing:
        raise ValueError("日线数据缺少字段：{}".format(", ".join(sorted(missing))))
    table = dataset.to_table(
        columns=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close"],
        filter=ds.field("ts_code") == ts_code,
    )
    df = table.to_pandas()
    if df.empty:
        raise ValueError("本地日线中没有 {} 的数据".format(ts_code))
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise")
    df = df.sort_values("trade_date").drop_duplicates("trade_date", keep="last")
    df = df.reset_index(drop=True)
    price_columns = ["open", "high", "low", "close", "pre_close"]
    for column in price_columns:
        df[column] = pd.to_numeric(df[column], errors="raise")
    invalid = df[price_columns].isna().any(axis=1) | (df[price_columns] <= 0).any(axis=1)
    if invalid.any():
        dates = df.loc[invalid, "trade_date"].dt.strftime("%Y-%m-%d").tolist()
        raise ValueError("行情包含空值或非正价格，日期示例：{}".format(dates[:5]))
    if len(df) < 30:
        raise ValueError("{} 仅有 {} 根 K 线，公式至少需要 30 根".format(ts_code, len(df)))
    return df


def load_stock_name(ts_code):
    """股票名称为可选信息，读取失败不影响回测。"""
    path = DATA_DIR / "stock_basic.parquet"
    if not path.is_file():
        return ""
    try:
        basic = pd.read_parquet(path, columns=["ts_code", "name"])
        matched = basic.loc[basic["ts_code"] == ts_code, "name"]
        return str(matched.iloc[0]) if not matched.empty else ""
    except Exception:
        return ""


def prepare_signals(df):
    """重建后复权价格，并逐项复刻 BUY/SELL 公式。"""
    df = df.copy()
    factors = [1.0]
    for i in range(1, len(df)):
        factors.append(factors[-1] * df.at[i - 1, "close"] / df.at[i, "pre_close"])
    df["factor"] = factors
    for column in ["open", "high", "low", "close"]:
        df["a_" + column] = df[column] * df["factor"]

    source = (
        3 * df["a_close"] + df["a_high"] + df["a_low"] + df["a_open"]
    ) / 6
    df["MM"] = source.ewm(span=21, adjust=False).mean()
    df["FL"] = df["MM"].rolling(5, min_periods=1).max()
    df["UP"] = df["MM"] >= df["FL"]
    df["TS"] = np.arange(1, len(df) + 1) >= 30
    previous_up = df["UP"].shift(1, fill_value=False)
    df["BUY"] = df["UP"] & ~previous_up & df["TS"]
    df["SELL"] = ~df["UP"] & previous_up & df["TS"]
    return df


def run_backtest(df):
    """信号日收盘计算、下一交易日开盘成交，逐日按收盘价估值。"""
    start_index = int(np.flatnonzero(df["TS"].to_numpy())[0])
    start_date = df.at[start_index, "trade_date"]
    end_date = df.at[len(df) - 1, "trade_date"]
    cash = INITIAL_CAPITAL
    shares = 0.0
    pending = None
    open_trade = None
    trades = []
    nav_records = []
    total_commission = 0.0
    total_tax = 0.0

    for i in range(start_index, len(df)):
        row = df.iloc[i]
        if pending is not None:
            price = row["a_open"]
            if pending == "BUY" and shares == 0:
                starting_cash = cash
                proportional_shares = cash / (price * (1 + COMMISSION_RATE))
                if proportional_shares * price * COMMISSION_RATE >= MIN_COMMISSION:
                    shares = proportional_shares
                    gross = shares * price
                    commission = gross * COMMISSION_RATE
                else:
                    shares = (cash - MIN_COMMISSION) / price
                    gross = shares * price
                    commission = MIN_COMMISSION
                cash -= gross + commission
                total_commission += commission
                open_trade = {
                    "buy_signal": df.at[i - 1, "trade_date"],
                    "buy_date": row["trade_date"],
                    "buy_price": price,
                    "starting_cash": starting_cash,
                    "buy_commission": commission,
                }
                trades.append(dict(open_trade, side="BUY"))
            elif pending == "SELL" and shares > 0:
                gross = shares * price
                commission = max(MIN_COMMISSION, gross * COMMISSION_RATE)
                tax_rate = 0.001 if row["trade_date"] < pd.Timestamp("2023-08-28") else 0.0005
                tax = gross * tax_rate
                cash += gross - commission - tax
                total_commission += commission
                total_tax += tax
                trade_return = cash / open_trade["starting_cash"] - 1
                trades.append(dict(
                    open_trade,
                    side="SELL",
                    sell_signal=df.at[i - 1, "trade_date"],
                    sell_date=row["trade_date"],
                    sell_price=price,
                    sell_commission=commission,
                    tax=tax,
                    trade_return=trade_return,
                    holding_days=(row["trade_date"] - open_trade["buy_date"]).days,
                    ending_cash=cash,
                ))
                shares = 0.0
                open_trade = None
            pending = None

        nav_records.append({
            "date": row["trade_date"],
            "value": cash + shares * row["a_close"],
            "position": int(shares > 0),
        })
        if row["BUY"] and shares == 0:
            pending = "BUY"
        elif row["SELL"] and shares > 0:
            pending = "SELL"

    nav = pd.DataFrame(nav_records).set_index("date")
    nav["return"] = nav["value"].pct_change().fillna(0.0)
    nav["drawdown"] = nav["value"] / nav["value"].cummax() - 1
    closed = [trade for trade in trades if trade["side"] == "SELL"]
    final_value = float(nav["value"].iloc[-1])
    years = (end_date - start_date).days / 365.2425
    annual_volatility = float(nav["return"].std(ddof=1) * np.sqrt(252))
    sharpe = (
        (float(nav["return"].mean()) * 252 - RISK_FREE_RATE) / annual_volatility
        if annual_volatility > 0 else np.nan
    )
    profits = sum(
        max(0.0, trade["ending_cash"] - trade["starting_cash"]) for trade in closed
    )
    losses = -sum(
        min(0.0, trade["ending_cash"] - trade["starting_cash"]) for trade in closed
    )
    buyhold_return = float(df["a_close"].iloc[-1] / df["a_close"].iloc[start_index] - 1)
    buyhold_nav = df["a_close"].iloc[start_index:].to_numpy() / df["a_close"].iloc[start_index]
    buyhold_drawdown = float((buyhold_nav / np.maximum.accumulate(buyhold_nav) - 1).min())

    yearly = []
    previous_value = INITIAL_CAPITAL
    for year, value in nav["value"].groupby(nav.index.year).last().items():
        yearly.append((int(year), float(value / previous_value - 1)))
        previous_value = value

    return {
        "start_date": start_date,
        "end_date": end_date,
        "bars": len(df),
        "years": years,
        "final_value": final_value,
        "total_return": final_value / INITIAL_CAPITAL - 1,
        "cagr": (final_value / INITIAL_CAPITAL) ** (1 / years) - 1,
        "max_drawdown": float(nav["drawdown"].min()),
        "max_drawdown_date": nav["drawdown"].idxmin(),
        "annual_volatility": annual_volatility,
        "sharpe": sharpe,
        "closed_trades": closed,
        "win_rate": (
            sum(trade["trade_return"] > 0 for trade in closed) / len(closed)
            if closed else np.nan
        ),
        "profit_factor": profits / losses if losses > 0 else np.inf,
        "average_trade_return": (
            float(np.mean([trade["trade_return"] for trade in closed]))
            if closed else np.nan
        ),
        "median_trade_return": (
            float(np.median([trade["trade_return"] for trade in closed]))
            if closed else np.nan
        ),
        "average_holding_days": (
            float(np.mean([trade["holding_days"] for trade in closed]))
            if closed else np.nan
        ),
        "exposure": float(nav["position"].mean()),
        "total_commission": total_commission,
        "total_tax": total_tax,
        "open_trade": open_trade,
        "yearly": yearly,
        "buyhold_return": buyhold_return,
        "buyhold_cagr": (1 + buyhold_return) ** (1 / years) - 1,
        "buyhold_max_drawdown": buyhold_drawdown,
    }


def percent(value):
    return "N/A" if pd.isna(value) else "{:+.2f}%".format(value * 100)


def print_report(ts_code, stock_name, df, result):
    title = ts_code + (" " + stock_name if stock_name else "")
    print("\n{}：趋势公式回测".format(title))
    print("=" * 72)
    print("数据区间：{} 至 {}（{} 根日 K）".format(
        df["trade_date"].iloc[0].date(), df["trade_date"].iloc[-1].date(), len(df)
    ))
    print("回测区间：{} 至 {}（第 30 根 K 线起）".format(
        result["start_date"].date(), result["end_date"].date()
    ))
    print("初始资金：{:,.2f} 元；期末资产：{:,.2f} 元".format(
        INITIAL_CAPITAL, result["final_value"]
    ))
    print("\n核心指标")
    print("-" * 72)
    print("指标                 策略             同期买入持有")
    print("累计收益           {:>12}       {:>12}".format(
        percent(result["total_return"]), percent(result["buyhold_return"])
    ))
    print("年化收益           {:>12}       {:>12}".format(
        percent(result["cagr"]), percent(result["buyhold_cagr"])
    ))
    print("最大回撤           {:>12}       {:>12}".format(
        percent(result["max_drawdown"]), percent(result["buyhold_max_drawdown"])
    ))
    print("策略最大回撤谷底：{}".format(result["max_drawdown_date"].date()))
    print("年化波动率：{:.2%}；夏普比率（无风险利率 3%）：{:.2f}".format(
        result["annual_volatility"], result["sharpe"]
    ))

    closed = result["closed_trades"]
    print("\n交易统计")
    print("-" * 72)
    print("完整买卖：{} 次；胜率：{}；盈亏比：{:.2f}".format(
        len(closed), percent(result["win_rate"]), result["profit_factor"]
    ))
    print("单笔平均：{}；单笔中位数：{}；平均持仓：{:.1f} 个自然日".format(
        percent(result["average_trade_return"]),
        percent(result["median_trade_return"]), result["average_holding_days"],
    ))
    print("持仓时间占比：{:.2%}；佣金：{:,.2f} 元；印花税：{:,.2f} 元".format(
        result["exposure"], result["total_commission"], result["total_tax"]
    ))
    if result["open_trade"]:
        trade = result["open_trade"]
        unrealized = result["final_value"] / trade["starting_cash"] - 1
        print("当前状态：持仓；买入日：{}；本回合浮动收益：{}".format(
            trade["buy_date"].date(), percent(unrealized)
        ))
    else:
        print("当前状态：空仓")

    print("\n分年度收益（首年/末年可能不是完整年度）")
    print("-" * 72)
    for year, value in result["yearly"]:
        print("{}：{}".format(year, percent(value)))

    print("\n最近五次完整交易")
    print("-" * 72)
    if not closed:
        print("暂无完整交易")
    for trade in closed[-5:]:
        print("{} 买入 → {} 卖出：{}（{} 天）".format(
            trade["buy_date"].date(), trade["sell_date"].date(),
            percent(trade["trade_return"]), trade["holding_days"],
        ))

    print("\n最近十个信号")
    print("-" * 72)
    signals = df[df["BUY"] | df["SELL"]].tail(10)
    for _, row in signals.iterrows():
        signal = "BUY" if row["BUY"] else "SELL"
        print("{}  {:<4}  当日不复权收盘：{:.2f}".format(
            row["trade_date"].date(), signal, row["close"]
        ))
    print("\n口径：后复权 OHLC；收盘出信号，下一交易日开盘成交；单票全仓。")
    print("费用：佣金万三且最低 5 元；卖出印花税按历史税率；无滑点。")
    print("未模拟涨跌停无法成交及整手限制；板块和资金流向不参与信号。")


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if not args or (len(args) == 1 and args[0].lower() in {"help", "-h", "--help"}):
        print(HELP_TEXT)
        return 0
    if len(args) != 1:
        print("错误：只接受一个股票代码参数。\n", file=sys.stderr)
        print(HELP_TEXT, file=sys.stderr)
        return 2
    try:
        ts_code = normalize_stock_code(args[0])
        daily = load_daily(ts_code)
        stock_name = load_stock_name(ts_code)
        with_signals = prepare_signals(daily)
        result = run_backtest(with_signals)
        print_report(ts_code, stock_name, with_signals, result)
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print("错误：{}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
