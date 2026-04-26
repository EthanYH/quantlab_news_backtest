import argparse, sys, yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantlab_news.upbit_data import fetch_hourly_ohlcv

ap = argparse.ArgumentParser()
ap.add_argument("--config", default="config.yaml")
ap.add_argument("--out", default="data/raw/upbit_hourly.csv")
args = ap.parse_args()
cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
df = fetch_hourly_ohlcv(cfg["market"], cfg["start"], cfg["end"])
Path(args.out).parent.mkdir(parents=True, exist_ok=True)
df.to_csv(args.out, index=False, encoding="utf-8-sig")
print(f"saved {args.out}: {len(df)} rows")
