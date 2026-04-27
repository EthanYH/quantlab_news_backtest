import argparse, json, sys, yaml
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantlab_news.sentiment import add_sentiment, daily_features
from quantlab_news.backtest import run_backtest, relation_report

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--price", default="data/raw/upbit_hourly.csv")
ap.add_argument("--news", default="data/raw/news.csv")
ap.add_argument("--outdir", default="reports")
args = ap.parse_args()
cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
price = pd.read_csv(args.price)
news = pd.read_csv(args.news)
news = add_sentiment(news)
feat = daily_features(
    news,
    cutoff_time=cfg["entry_time"],
    signal_window=cfg.get("signal_window", "cutoff_to_cutoff"),
)
rel = relation_report(price, feat)
trades, stats = run_backtest(
    price, feat,
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
out = Path(args.outdir); out.mkdir(parents=True, exist_ok=True)
news.to_csv(out/"news_scored.csv", index=False, encoding="utf-8-sig")
feat.to_csv(out/"daily_features.csv", index=False, encoding="utf-8-sig")
rel.to_csv(out/"relation_dataset.csv", index=False, encoding="utf-8-sig")
trades.to_csv(out/"trades.csv", index=False, encoding="utf-8-sig")
(out/"stats.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(stats, indent=2, ensure_ascii=False))
if not rel.empty:
    print("correlation sentiment_mean vs day_ret:", rel[["sentiment_mean", "day_ret"]].corr().iloc[0,1])
    signal_mask = (
        (rel.sentiment_mean >= cfg["sentiment_threshold"])
        & (rel.article_count >= cfg["min_article_count"])
        & (rel.positive_ratio >= cfg.get("min_positive_ratio", 0.0))
    )
    if cfg.get("max_negative_count") is not None:
        signal_mask &= rel.negative_count <= cfg["max_negative_count"]
    print("P(hit 1% | signal filters):", rel[signal_mask]["hit_1pct"].mean())
