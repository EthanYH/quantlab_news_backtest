from __future__ import annotations

import pandas as pd


def run_backtest(
    price_hourly: pd.DataFrame,
    features: pd.DataFrame,
    *,
    entry_time="08:50",
    take_profit_pct=0.01,
    stop_loss_pct=None,
    max_hold_hours=24,
    fee_rate=0.0005,
    sentiment_threshold=0.2,
    min_article_count=3,
    min_positive_ratio=0.0,
    max_negative_count=None,
) -> tuple[pd.DataFrame, dict]:
    p = price_hourly.copy()
    p["ts"] = pd.to_datetime(p["ts"])
    if p["ts"].dt.tz is None:
        p["ts"] = p["ts"].dt.tz_localize("Asia/Seoul")
    else:
        p["ts"] = p["ts"].dt.tz_convert("Asia/Seoul")
    p = p.sort_values("ts")
    f = features.copy()
    f["signal_date"] = pd.to_datetime(f["signal_date"]).dt.date
    mask = (
        (f["sentiment_mean"] >= sentiment_threshold)
        & (f["article_count"] >= min_article_count)
        & (f["positive_ratio"] >= min_positive_ratio)
    )
    if max_negative_count is not None:
        mask &= f["negative_count"] <= max_negative_count
    signals = f[mask]
    trades = []
    eh, em = map(int, entry_time.split(":"))
    for _, s in signals.iterrows():
        day = pd.Timestamp(s["signal_date"], tz="Asia/Seoul")
        entry_ts = day + pd.Timedelta(hours=eh, minutes=em)
        # hourly candles: use first candle at/after 08:50, usually 09:00 candle open
        window = p[(p["ts"] >= entry_ts) & (p["ts"] <= entry_ts + pd.Timedelta(hours=max_hold_hours))]
        if window.empty:
            continue
        entry = window.iloc[0]
        entry_price = float(entry["open"])
        tp = entry_price * (1 + take_profit_pct)
        sl = entry_price * (1 - stop_loss_pct) if stop_loss_pct is not None else None
        exit_row = window.iloc[-1]
        exit_price = float(exit_row["close"])
        reason = "time_exit"
        for _, row in window.iterrows():
            if sl is not None and float(row["low"]) <= sl:
                exit_row, exit_price, reason = row, sl, "stop_loss"
                break
            if float(row["high"]) >= tp:
                exit_row, exit_price, reason = row, tp, "take_profit"
                break
        gross_ret = exit_price / entry_price - 1
        net_ret = gross_ret - (fee_rate * 2)
        trades.append({
            "signal_date": s["signal_date"],
            "entry_ts": entry["ts"],
            "exit_ts": exit_row["ts"],
            "entry_price": entry_price,
            "exit_price": exit_price,
            "reason": reason,
            "gross_ret": gross_ret,
            "net_ret": net_ret,
            "article_count": s["article_count"],
            "sentiment_mean": s["sentiment_mean"],
            "positive_ratio": s["positive_ratio"],
            "negative_count": s["negative_count"],
        })
    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return trades_df, {"trades": 0, "message": "No trades."}
    equity = (1 + trades_df["net_ret"]).cumprod()
    peak = equity.cummax()
    mdd = ((equity / peak) - 1).min()
    stats = {
        "trades": int(len(trades_df)),
        "win_rate": float((trades_df["net_ret"] > 0).mean()),
        "avg_net_ret": float(trades_df["net_ret"].mean()),
        "total_return": float(equity.iloc[-1] - 1),
        "mdd": float(mdd),
        "take_profit_rate": float((trades_df["reason"] == "take_profit").mean()),
    }
    return trades_df, stats


def relation_report(price_hourly: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    p = price_hourly.copy()
    p["ts"] = pd.to_datetime(p["ts"])
    if p["ts"].dt.tz is None:
        p["ts"] = p["ts"].dt.tz_localize("Asia/Seoul")
    p["date"] = p["ts"].dt.tz_convert("Asia/Seoul").dt.date
    daily = p.groupby("date").agg(open=("open", "first"), close=("close", "last"), high=("high", "max"), low=("low", "min"), volume=("volume", "sum")).reset_index()
    daily["day_ret"] = daily["close"] / daily["open"] - 1
    daily["hit_1pct"] = (daily["high"] / daily["open"] - 1) >= 0.01
    f = features.copy()
    f["date"] = pd.to_datetime(f["signal_date"]).dt.date
    out = f.merge(daily, on="date", how="inner")
    return out
