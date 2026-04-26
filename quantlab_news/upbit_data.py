from __future__ import annotations
import time
from datetime import datetime
import pandas as pd
import pyupbit


def fetch_hourly_ohlcv(market: str, start: str, end: str, sleep_sec: float = 0.12) -> pd.DataFrame:
    """Fetch Upbit hourly OHLCV. pyupbit returns at most 200 rows per request."""
    start_ts = pd.Timestamp(start, tz="Asia/Seoul")
    end_ts = pd.Timestamp(end, tz="Asia/Seoul")
    to = end_ts
    chunks = []
    while True:
        df = pyupbit.get_ohlcv(market, interval="minute60", to=to.to_pydatetime(), count=200)
        if df is None or df.empty:
            break
        df.index = pd.to_datetime(df.index).tz_localize("Asia/Seoul", nonexistent="shift_forward", ambiguous="NaT") if df.index.tz is None else df.index.tz_convert("Asia/Seoul")
        chunks.append(df)
        oldest = df.index.min()
        if oldest <= start_ts:
            break
        to = oldest - pd.Timedelta(seconds=1)
        time.sleep(sleep_sec)
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks).sort_index()
    out = out[~out.index.duplicated(keep="first")]
    out = out[(out.index >= start_ts) & (out.index <= end_ts)]
    out.index.name = "ts"
    return out.reset_index()
