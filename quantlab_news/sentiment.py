from __future__ import annotations

import re

import pandas as pd


POS = [
    "positive", "bull", "bullish", "surge", "rally", "breakout", "recover",
    "rebound", "gain", "jump", "soar", "approve", "approval", "inflow",
    "adoption", "accumulate", "institutional", "upgrade", "record high",
    "all-time high", "ath", "etf inflow",
]
NEG = [
    "negative", "bear", "bearish", "crash", "dump", "plunge", "selloff",
    "liquidation", "hack", "exploit", "fraud", "lawsuit", "ban", "outflow",
    "reject", "rejection", "regulation", "probe", "investigation", "fine",
    "bankrupt", "default", "panic", "risk-off", "etf outflow",
]
TAG_RE = re.compile(r"<[^>]+>")


def clean_text(s: str) -> str:
    return TAG_RE.sub(" ", str(s)).replace("&quot;", '"').replace("&amp;", "&")


def score_text(text: str) -> float:
    t = clean_text(text).lower()
    pos = sum(1 for w in POS if w.lower() in t)
    neg = sum(1 for w in NEG if w.lower() in t)
    if pos + neg == 0:
        return 0.0
    return (pos - neg) / (pos + neg)


def add_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    summary_col = "summary" if "summary" in out.columns else "content"
    out["text"] = (
        out["title"].fillna("").astype(str)
        + " "
        + out[summary_col].fillna("").astype(str)
    )
    out["sentiment"] = out["text"].map(score_text)
    out["sent_label"] = out["sentiment"].apply(
        lambda x: "pos" if x > 0 else ("neg" if x < 0 else "neu")
    )
    return out


def daily_features(news: pd.DataFrame, cutoff_time: str = "08:50") -> pd.DataFrame:
    """Build features available by cutoff_time. Signal date means trading date at cutoff_time."""
    time_col = "published_at" if "published_at" in news.columns else "time"
    df = news.dropna(subset=[time_col]).copy()
    df["published_at"] = pd.to_datetime(df[time_col])
    cutoff_h, cutoff_m = map(int, cutoff_time.split(":"))

    if df["published_at"].dt.tz is not None:
        local = df["published_at"].dt.tz_convert("Asia/Seoul")
    else:
        local = df["published_at"]

    base_date = local.dt.date
    after_cutoff = (
        (local.dt.hour > cutoff_h)
        | ((local.dt.hour == cutoff_h) & (local.dt.minute > cutoff_m))
    )
    df["signal_date"] = pd.to_datetime(base_date) + pd.to_timedelta(
        after_cutoff.astype(int),
        unit="D",
    )
    g = df.groupby("signal_date")
    feat = g.agg(
        article_count=("sentiment", "size"),
        sentiment_mean=("sentiment", "mean"),
        sentiment_sum=("sentiment", "sum"),
        positive_count=("sent_label", lambda x: (x == "pos").sum()),
        negative_count=("sent_label", lambda x: (x == "neg").sum()),
    ).reset_index()
    feat["positive_ratio"] = feat["positive_count"] / feat["article_count"].clip(lower=1)
    return feat
