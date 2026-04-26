import argparse
import pandas as pd
import feedparser
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

QUERY = '(bitcoin OR btc OR cryptocurrency OR crypto OR ethereum OR eth OR xrp OR binance OR "spot bitcoin etf" OR "crypto regulation") -football -baseball -softball -basketball -champion -tigers'

def parse_entry(entry):
    title = entry.get("title", "")
    summary = entry.get("summary", "")

    published = entry.get("published_parsed", None)
    if published:
        dt = datetime(*published[:6], tzinfo=timezone.utc)
    else:
        return None

    return {
        "time": dt,
        "title": title,
        "content": summary
    }

def collect_news(days=90):
    q = quote_plus(QUERY)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

    feed = feedparser.parse(url)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    rows = []
    for entry in feed.entries:
        parsed = parse_entry(entry)
        if parsed and parsed["time"] >= cutoff:
            rows.append(parsed)

    df = pd.DataFrame(rows)
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--out", default="data/news/btc_news_3m.csv")
    args = parser.parse_args()

    df = collect_news(args.days)

    df = df.sort_values("time")
    df.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"[DONE] collected={len(df)} saved={args.out}")
    print(df.head())
    print(df.tail())

if __name__ == "__main__":
    main()