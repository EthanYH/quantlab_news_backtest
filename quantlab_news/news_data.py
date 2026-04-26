from __future__ import annotations

import os
import urllib.parse
from datetime import datetime, timezone
from typing import Iterable

import feedparser
import pandas as pd
import requests


DEFAULT_HEADERS = {
    "User-Agent": "quantlab-news-backtest/0.1 (+https://localhost)"
}


def _empty_news_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["published_at", "title", "summary", "source", "url"])


def _to_kst(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")


def _normalize_news_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_news_frame()

    out = df.copy()
    for col in ["published_at", "title", "summary", "source", "url"]:
        if col not in out.columns:
            out[col] = ""

    out["published_at"] = _to_kst(out["published_at"])
    out = out.dropna(subset=["published_at"])
    out = out[["published_at", "title", "summary", "source", "url"]]
    return out


def combine_news_frames(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """Combine source frames into the schema expected by the sentiment pipeline."""
    valid = [f for f in frames if f is not None and not f.empty]
    if not valid:
        return _empty_news_frame()

    out = pd.concat(valid, ignore_index=True)
    out = _normalize_news_frame(out)
    out = out.drop_duplicates(subset=["url", "title"], keep="first")
    return out.sort_values("published_at").reset_index(drop=True)


def fetch_google_news_rss(
    query: str,
    start: str | None = None,
    end: str | None = None,
    *,
    hl: str = "en-US",
    gl: str = "US",
    ceid: str = "US:en",
) -> pd.DataFrame:
    """Free Google News RSS collector, defaulting to US English results."""
    q = query
    if start:
        q += f" after:{start}"
    if end:
        q += f" before:{end}"

    url = (
        "https://news.google.com/rss/search?q="
        + urllib.parse.quote(q)
        + f"&hl={hl}&gl={gl}&ceid={urllib.parse.quote(ceid)}"
    )
    feed = feedparser.parse(url)
    rows = []
    for entry in feed.entries:
        rows.append({
            "published_at": getattr(entry, "published", ""),
            "title": getattr(entry, "title", ""),
            "summary": getattr(entry, "summary", ""),
            "source": getattr(getattr(entry, "source", {}), "title", "google_news"),
            "url": getattr(entry, "link", ""),
        })
    return _normalize_news_frame(pd.DataFrame(rows))


def fetch_naver_news(query: str, display: int = 100, start: int = 1) -> pd.DataFrame:
    """Naver Search API collector. Requires NAVER_CLIENT_ID and NAVER_CLIENT_SECRET."""
    cid = os.getenv("NAVER_CLIENT_ID")
    secret = os.getenv("NAVER_CLIENT_SECRET")
    if not cid or not secret:
        raise RuntimeError("NAVER_CLIENT_ID and NAVER_CLIENT_SECRET are required.")

    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": secret}
    params = {"query": query, "display": display, "start": start, "sort": "date"}
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()

    rows = []
    for item in r.json().get("items", []):
        rows.append({
            "published_at": item.get("pubDate"),
            "title": item.get("title", ""),
            "summary": item.get("description", ""),
            "source": "naver",
            "url": item.get("link", ""),
        })
    return _normalize_news_frame(pd.DataFrame(rows))


def fetch_reddit_posts(
    query: str,
    *,
    subreddits: list[str] | None = None,
    limit: int = 100,
    time_filter: str = "week",
) -> pd.DataFrame:
    """Collect recent Reddit posts via Reddit's public JSON endpoints."""
    subs = subreddits or ["CryptoCurrency", "Bitcoin", "ethereum", "CryptoMarkets"]
    rows = []
    per_sub_limit = max(1, min(100, limit))
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    for sub in subs:
        url = f"https://www.reddit.com/r/{sub}/search.json"
        params = {
            "q": query,
            "restrict_sr": 1,
            "sort": "new",
            "t": time_filter,
            "limit": per_sub_limit,
        }
        r = session.get(url, params=params, timeout=20)
        r.raise_for_status()
        for child in r.json().get("data", {}).get("children", []):
            post = child.get("data", {})
            created = post.get("created_utc")
            published_at = (
                datetime.fromtimestamp(created, tz=timezone.utc).isoformat()
                if created is not None
                else ""
            )
            rows.append({
                "published_at": published_at,
                "title": post.get("title", ""),
                "summary": post.get("selftext", ""),
                "source": f"reddit:r/{sub}",
                "url": "https://www.reddit.com" + post.get("permalink", ""),
            })
    return _normalize_news_frame(pd.DataFrame(rows))


def fetch_twitter_recent(
    query: str,
    *,
    max_results: int = 100,
    lang: str | None = "en",
) -> pd.DataFrame:
    """Collect recent tweets from X API v2. Requires TWITTER_BEARER_TOKEN or X_BEARER_TOKEN."""
    token = os.getenv("TWITTER_BEARER_TOKEN") or os.getenv("X_BEARER_TOKEN")
    if not token:
        raise RuntimeError("TWITTER_BEARER_TOKEN or X_BEARER_TOKEN is required for Twitter/X.")

    q = query
    if lang and f"lang:{lang}" not in q:
        q = f"({q}) lang:{lang}"

    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": q,
        "max_results": max(10, min(100, max_results)),
        "tweet.fields": "created_at,author_id,lang,public_metrics",
    }
    headers = {"Authorization": f"Bearer {token}", **DEFAULT_HEADERS}
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()

    rows = []
    for tweet in r.json().get("data", []):
        tweet_id = tweet.get("id", "")
        rows.append({
            "published_at": tweet.get("created_at", ""),
            "title": tweet.get("text", ""),
            "summary": tweet.get("text", ""),
            "source": "twitter",
            "url": f"https://twitter.com/i/web/status/{tweet_id}" if tweet_id else "",
        })
    return _normalize_news_frame(pd.DataFrame(rows))
