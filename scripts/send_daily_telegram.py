import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantlab_news.news_data import combine_news_frames, fetch_reddit_posts
from quantlab_news.sentiment import add_sentiment


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def send_telegram(message: str, *, token: str, chat_id: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True,
        },
        timeout=20,
    )
    if not response.ok:
        detail = response.text[:500]
        raise RuntimeError(f"Telegram sendMessage failed: HTTP {response.status_code} {detail}")


def signal_mask(row: pd.Series, cfg: dict) -> bool:
    ok = (
        float(row["sentiment_mean"]) >= float(cfg["sentiment_threshold"])
        and int(row["article_count"]) >= int(cfg["min_article_count"])
        and float(row["positive_ratio"]) >= float(cfg.get("min_positive_ratio", 0.0))
    )
    max_negative_count = cfg.get("max_negative_count")
    if max_negative_count is not None:
        ok = ok and int(row["negative_count"]) <= int(max_negative_count)
    return ok


def build_message(window_news: pd.DataFrame, cfg: dict, now: pd.Timestamp) -> str:
    scored = add_sentiment(window_news)
    article_count = len(scored)
    positive_count = int((scored["sent_label"] == "pos").sum())
    negative_count = int((scored["sent_label"] == "neg").sum())
    sentiment_mean = float(scored["sentiment"].mean()) if article_count else 0.0
    positive_ratio = positive_count / max(article_count, 1)
    row = pd.Series({
        "article_count": article_count,
        "sentiment_mean": sentiment_mean,
        "positive_ratio": positive_ratio,
        "negative_count": negative_count,
    })
    should_enter = signal_mask(row, cfg)

    title = "BUY signal" if should_enter else "NO TRADE"
    lines = [
        f"[QuantLab] {title}",
        f"time: {now.strftime('%Y-%m-%d %H:%M %Z')}",
        f"window: previous cutoff -> current {cfg['entry_time']}",
        "",
        f"articles: {article_count}",
        f"sentiment_mean: {sentiment_mean:.4f}",
        f"positive_ratio: {positive_ratio:.2%}",
        f"positive/negative: {positive_count}/{negative_count}",
        "",
        "strategy:",
        f"TP/SL: {float(cfg['take_profit_pct']) * 100:.2f}% / {float(cfg['stop_loss_pct']) * 100:.2f}%",
        f"threshold: {cfg['sentiment_threshold']}",
        f"min_articles: {cfg['min_article_count']}",
        f"max_negative_count: {cfg.get('max_negative_count')}",
    ]

    if not scored.empty:
        lines.extend(["", "top positive:"])
        top_pos = scored.sort_values("sentiment", ascending=False).head(5)
        for _, item in top_pos.iterrows():
            lines.append(f"- {str(item['title'])[:120]}")

        lines.extend(["", "latest:"])
        latest = scored.sort_values("published_at", ascending=False).head(5)
        for _, item in latest.iterrows():
            source = str(item.get("source", ""))
            lines.append(f"- [{source}] {str(item['title'])[:110]}")

    return "\n".join(lines)[:3900]


def collect_reddit(cfg: dict) -> pd.DataFrame:
    reddit_cfg = cfg.get("reddit", {})
    queries = as_list(reddit_cfg.get("queries")) or [reddit_cfg.get("query", cfg["news_query"])]
    frames = []
    for query in queries:
        frames.append(
            fetch_reddit_posts(
                query,
                subreddits=reddit_cfg.get("subreddits"),
                limit=reddit_cfg.get("limit", 100),
                time_filter=reddit_cfg.get("time_filter", "week"),
            )
        )
    return combine_news_frames(frames)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--out", default="data/raw/daily_reddit_signal.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not args.dry_run and (not token or not chat_id):
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required.")

    now = pd.Timestamp.now(tz="Asia/Seoul")
    cutoff_h, cutoff_m = map(int, cfg["entry_time"].split(":"))
    cutoff = now.normalize() + pd.Timedelta(hours=cutoff_h, minutes=cutoff_m)
    if now < cutoff:
        cutoff = cutoff - pd.Timedelta(days=1)
    window_start = cutoff - pd.Timedelta(days=1)
    window_end = cutoff

    news = collect_reddit(cfg)
    if news.empty:
        window_news = news.copy()
    else:
        local_time = pd.to_datetime(news["published_at"]).dt.tz_convert("Asia/Seoul")
        window_news = news[(local_time > window_start) & (local_time <= window_end)].copy()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    window_news.to_csv(args.out, index=False, encoding="utf-8-sig")

    message = build_message(window_news, cfg, cutoff)
    if args.dry_run:
        print(message)
    else:
        send_telegram(message, token=token, chat_id=chat_id)
        print(f"sent telegram message with {len(window_news)} articles")


if __name__ == "__main__":
    main()
