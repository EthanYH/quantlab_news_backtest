import argparse
import itertools
import json
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantlab_news.backtest import run_backtest
from quantlab_news.sentiment import add_sentiment, daily_features


def parse_float_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def parse_int_list(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_optional_int_list(value: str) -> list[int | None]:
    out = []
    for item in value.split(","):
        item = item.strip().lower()
        if not item:
            continue
        out.append(None if item in {"none", "null"} else int(item))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--price", default="data/binance/BTCUSDT_1h_backtest.csv")
    ap.add_argument("--news", default="data/raw/news_reddit.csv")
    ap.add_argument("--out", default="reports/parameter_sweep.csv")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--thresholds", type=parse_float_list, default="0.05,0.1,0.15,0.2,0.25,0.3")
    ap.add_argument("--min-counts", type=parse_int_list, default="1,2,3,5")
    ap.add_argument("--take-profits", type=parse_float_list, default="0.007,0.01,0.012")
    ap.add_argument("--stop-losses", type=parse_float_list, default="0.008,0.01,0.012")
    ap.add_argument("--positive-ratios", type=parse_float_list, default="0,0.2,0.3,0.4")
    ap.add_argument("--max-negatives", type=parse_optional_int_list, default="none,0,1,2")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    price = pd.read_csv(args.price)
    news = add_sentiment(pd.read_csv(args.news))
    feat = daily_features(news, cutoff_time=cfg["entry_time"])

    rows = []
    grid = itertools.product(
        args.thresholds,
        args.min_counts,
        args.take_profits,
        args.stop_losses,
        args.positive_ratios,
        args.max_negatives,
    )
    for threshold, min_count, take_profit, stop_loss, positive_ratio, max_negative in grid:
        trades, stats = run_backtest(
            price,
            feat,
            entry_time=cfg["entry_time"],
            take_profit_pct=take_profit,
            stop_loss_pct=stop_loss,
            max_hold_hours=cfg["max_hold_hours"],
            fee_rate=cfg["fee_rate"],
            sentiment_threshold=threshold,
            min_article_count=min_count,
            min_positive_ratio=positive_ratio,
            max_negative_count=max_negative,
        )
        stats = {
            "trades": 0,
            "win_rate": 0.0,
            "avg_net_ret": 0.0,
            "total_return": 0.0,
            "mdd": 0.0,
            "take_profit_rate": 0.0,
            **stats,
        }
        row = {
            "sentiment_threshold": threshold,
            "min_article_count": min_count,
            "take_profit_pct": take_profit,
            "stop_loss_pct": stop_loss,
            "min_positive_ratio": positive_ratio,
            "max_negative_count": max_negative,
            **stats,
        }
        if not trades.empty:
            row["profit_factor"] = (
                trades.loc[trades["net_ret"] > 0, "net_ret"].sum()
                / abs(trades.loc[trades["net_ret"] < 0, "net_ret"].sum())
                if (trades["net_ret"] < 0).any()
                else float("inf")
            )
        else:
            row["profit_factor"] = 0.0
        rows.append(row)

    out = pd.DataFrame(rows)
    out = out.sort_values(
        ["total_return", "mdd", "trades"],
        ascending=[False, False, False],
    )
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"saved {args.out}: {len(out)} rows")
    print(json.dumps(out.head(args.top).to_dict(orient="records"), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
