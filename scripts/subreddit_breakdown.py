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


def stats_row(
    *,
    label: str,
    mode: str,
    source_count: int,
    news_rows: int,
    price: pd.DataFrame,
    news: pd.DataFrame,
    cfg: dict,
) -> dict:
    if news.empty:
        return {
            "label": label,
            "mode": mode,
            "source_count": source_count,
            "news_rows": news_rows,
            "trades": 0,
            "win_rate": 0.0,
            "avg_net_ret": 0.0,
            "total_return": 0.0,
            "mdd": 0.0,
            "take_profit_rate": 0.0,
            "profit_factor": 0.0,
        }

    scored = add_sentiment(news)
    feat = daily_features(
        scored,
        cutoff_time=cfg["entry_time"],
        signal_window=cfg.get("signal_window", "cutoff_to_cutoff"),
    )
    trades, stats = run_backtest(
        price,
        feat,
        entry_time=cfg["entry_time"],
        take_profit_pct=cfg["take_profit_pct"],
        stop_loss_pct=cfg.get("stop_loss_pct"),
        max_hold_hours=cfg["max_hold_hours"],
        fee_rate=cfg["fee_rate"],
        sentiment_threshold=cfg["sentiment_threshold"],
        min_article_count=cfg["min_article_count"],
        min_positive_ratio=cfg.get("min_positive_ratio", 0.0),
        max_negative_count=cfg.get("max_negative_count"),
    )
    row = {
        "label": label,
        "mode": mode,
        "source_count": source_count,
        "news_rows": news_rows,
        "trades": 0,
        "win_rate": 0.0,
        "avg_net_ret": 0.0,
        "total_return": 0.0,
        "mdd": 0.0,
        "take_profit_rate": 0.0,
        **stats,
    }
    if not trades.empty:
        gross_profit = trades.loc[trades["net_ret"] > 0, "net_ret"].sum()
        gross_loss = abs(trades.loc[trades["net_ret"] < 0, "net_ret"].sum())
        row["profit_factor"] = gross_profit / gross_loss if gross_loss else float("inf")
    else:
        row["profit_factor"] = 0.0
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--price", default="data/binance/BTCUSDT_1h_backtest.csv")
    ap.add_argument("--news", default="data/raw/news_reddit.csv")
    ap.add_argument("--out", default="reports/subreddit_breakdown.csv")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--max-combo-size", type=int, default=0)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    price = pd.read_csv(args.price)
    news = pd.read_csv(args.news)
    if "source" not in news.columns:
        raise RuntimeError("news CSV must include a source column.")

    sources = sorted(news["source"].dropna().unique())
    rows = [
        stats_row(
            label="ALL",
            mode="all",
            source_count=len(sources),
            news_rows=len(news),
            price=price,
            news=news,
            cfg=cfg,
        )
    ]

    for source in sources:
        part = news[news["source"] == source]
        rows.append(
            stats_row(
                label=source,
                mode="only",
                source_count=1,
                news_rows=len(part),
                price=price,
                news=part,
                cfg=cfg,
            )
        )

    for source in sources:
        part = news[news["source"] != source]
        rows.append(
            stats_row(
                label=source,
                mode="exclude",
                source_count=len(sources) - 1,
                news_rows=len(part),
                price=price,
                news=part,
                cfg=cfg,
            )
        )

    for size in range(2, args.max_combo_size + 1):
        for combo in itertools.combinations(sources, size):
            part = news[news["source"].isin(combo)]
            rows.append(
                stats_row(
                    label=" + ".join(combo),
                    mode=f"combo_{size}",
                    source_count=size,
                    news_rows=len(part),
                    price=price,
                    news=part,
                    cfg=cfg,
                )
            )

    out = pd.DataFrame(rows)
    out = out.sort_values(["mode", "total_return", "trades"], ascending=[True, False, False])
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"saved {args.out}: {len(out)} rows")
    print(json.dumps(out.head(args.top).to_dict(orient="records"), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
