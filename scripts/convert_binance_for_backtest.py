import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    df = pd.read_csv(args.src)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_kst"] = df["timestamp"].dt.tz_convert("Asia/Seoul")

    out = df.rename(columns={
    "timestamp_kst": "ts"
})[["ts", "open", "high", "low", "close", "volume"]]
    
    out.to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"[DONE] rows={len(out)} saved={args.out}")
    print(out.head())
    print(out.tail())


if __name__ == "__main__":
    main()