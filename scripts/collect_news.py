import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantlab_news.news_data import (
    combine_news_frames,
    fetch_google_news_rss,
    fetch_naver_news,
    fetch_reddit_posts,
    fetch_twitter_recent,
)


SOURCE_CHOICES = {"google_rss", "naver", "reddit", "twitter"}


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def parse_sources(value: str) -> list[str]:
    if value == "all":
        return ["google_rss", "reddit", "twitter"]
    sources = [item.strip() for item in value.split(",") if item.strip()]
    invalid = sorted(set(sources) - SOURCE_CHOICES)
    if invalid:
        raise argparse.ArgumentTypeError(f"unknown source(s): {', '.join(invalid)}")
    return sources


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument(
        "--source",
        type=parse_sources,
        default=["google_rss"],
        help="One of google_rss, naver, reddit, twitter, all; comma-separated values are allowed.",
    )
    ap.add_argument("--out", default="data/raw/news.csv")
    ap.add_argument("--keep-going", action="store_true", help="Skip failed sources and save the rest.")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    query = cfg["news_query"]
    frames = []

    for source in args.source:
        try:
            if source == "naver":
                df = fetch_naver_news(query, display=cfg.get("naver_display", 100))
            elif source == "reddit":
                reddit_cfg = cfg.get("reddit", {})
                reddit_frames = []
                reddit_queries = as_list(reddit_cfg.get("queries")) or [reddit_cfg.get("query", query)]
                for reddit_query in reddit_queries:
                    reddit_frames.append(
                        fetch_reddit_posts(
                            reddit_query,
                            subreddits=reddit_cfg.get("subreddits"),
                            limit=reddit_cfg.get("limit", 100),
                            time_filter=reddit_cfg.get("time_filter", "week"),
                        )
                    )
                df = combine_news_frames(reddit_frames)
            elif source == "twitter":
                twitter_cfg = cfg.get("twitter", {})
                twitter_frames = []
                twitter_queries = as_list(twitter_cfg.get("queries")) or [twitter_cfg.get("query", query)]
                for twitter_query in twitter_queries:
                    twitter_frames.append(
                        fetch_twitter_recent(
                            twitter_query,
                            max_results=twitter_cfg.get("max_results", 100),
                            lang=twitter_cfg.get("lang", "en"),
                        )
                    )
                df = combine_news_frames(twitter_frames)
            else:
                google_cfg = cfg.get("google_news", {})
                google_frames = []
                google_queries = as_list(google_cfg.get("queries")) or [google_cfg.get("query", query)]
                for google_query in google_queries:
                    google_frames.append(
                        fetch_google_news_rss(
                            google_query,
                            cfg.get("start"),
                            cfg.get("end"),
                            hl=google_cfg.get("hl", "en-US"),
                            gl=google_cfg.get("gl", "US"),
                            ceid=google_cfg.get("ceid", "US:en"),
                        )
                    )
                df = combine_news_frames(google_frames)
            print(f"[OK] {source}: {len(df)} rows")
            frames.append(df)
        except Exception as exc:
            if not args.keep_going:
                raise
            print(f"[WARN] {source}: {exc}")

    df = combine_news_frames(frames)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"saved {args.out}: {len(df)} rows")


if __name__ == "__main__":
    main()
