"""板块热力图数据服务 — 读取真实 Parquet 分区并按行业聚合。"""
import numpy as np
import pandas as pd

from app.services.market_snapshot import load_latest_market_snapshot


class HeatmapService:
    """板块热力图数据聚合服务。"""

    def get_heatmap_data(self):
        """返回 (sectors_json, stocks_json)，个股按涨跌幅降序。"""
        df = load_latest_market_snapshot(include_market_cap=True).copy()
        if df.empty:
            raise ValueError("最新市场快照为空")
        df["trade_date"] = df["trade_date"].astype(str)
        trade_date = df["trade_date"].max()

        for column in ("close", "pct_chg", "total_mv", "net_mf_amount", "turnover_rate"):
            df[column] = pd.to_numeric(df[column], errors="coerce")

        # 过滤停牌/退市及无行业归属记录；缺失值不做零值填充。
        df = df[(df["close"] > 0) & df["industry"].notna()].copy()

        sectors = []
        for industry, group in df.groupby("industry"):
            weighted = group[(group["total_mv"] > 0) & group["pct_chg"].notna()]
            if weighted.empty:
                continue
            total_mv_sum = float(weighted["total_mv"].sum())
            avg_pct_chg = np.average(weighted["pct_chg"], weights=weighted["total_mv"])
            sectors.append({
                "name": industry,
                "avg_pct_chg": round(float(avg_pct_chg), 2),
                "total_mv": round(total_mv_sum, 2),
                "stock_count": int(len(group)),
                "up_count": int((group["pct_chg"] > 0).sum()),
                "down_count": int((group["pct_chg"] < 0).sum()),
                "net_mf_amount": round(float(group["net_mf_amount"].sum()), 2),
                "trade_date": trade_date,
            })

        sectors.sort(key=lambda item: item["total_mv"], reverse=True)

        stock_cols = [
            "ts_code", "name", "industry", "pct_chg", "close",
            "total_mv", "net_mf_amount", "turnover_rate",
        ]
        stocks_df = df[stock_cols].sort_values("pct_chg", ascending=False)
        stocks = stocks_df.astype(object).where(stocks_df.notna(), None).to_dict(orient="records")
        return sectors, stocks
