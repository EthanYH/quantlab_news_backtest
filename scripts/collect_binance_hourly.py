import argparse
import time
from datetime import datetime, timezone

import pandas as pd
import requests


BASE_URL = "https://api.binance.com/api/v3/klines"


def to_millis(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int | None = None):
    all_rows = []
    limit = 1000
    current = start_ms

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current,
            "limit": limit,
        }

        if end_ms:
            params["endTime"] = end_ms

        r = requests.get(BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data:
            break

        all_rows.extend(data)

        last_open_time = data[-1][0]
        next_time = last_open_time + 1

        if next_time <= current:
            break

        current = next_time

        print(
            f"[FETCH] rows={len(all_rows)} "
            f"last={datetime.fromtimestamp(last_open_time / 1000, tz=timezone.utc)}"
        )

        if len(data) < limit:
            break

        time.sleep(0.2)

    return all_rows


def klines_to_df(rows):
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    df = pd.DataFrame(rows, columns=columns)

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["timestamp_kst"] = df["timestamp"].dt.tz_convert("Asia/Seoul")

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[
        [
            "timestamp",
            "timestamp_kst",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ]
    ]

    df = df.drop_duplicates(subset=["timestamp"])
    df = df.sort_values("timestamp")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--start", default="2017-08-17")
    parser.add_argument("--end", default=None)
    parser.add_argument("--out", default="data/binance/BTCUSDT_1h.csv")
    args = parser.parse_args()

    start_ms = to_millis(args.start)
    end_ms = to_millis(args.end) if args.end else None

    rows = fetch_klines(args.symbol, args.interval, start_ms, end_ms)
    df = klines_to_df(rows)

    out_path = args.out
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("\n[DONE]")
    print(f"symbol   : {args.symbol}")
    print(f"interval : {args.interval}")
    print(f"rows     : {len(df)}")
    print(f"from     : {df['timestamp'].min()}")
    print(f"to       : {df['timestamp'].max()}")
    print(f"saved    : {out_path}")


if __name__ == "__main__":
    main()