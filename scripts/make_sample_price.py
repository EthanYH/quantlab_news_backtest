from pathlib import Path
import pandas as pd
idx = pd.date_range("2026-04-21 00:00", "2026-04-23 23:00", freq="1h", tz="Asia/Seoul")
rows=[]
base=100_000_000
for i, ts in enumerate(idx):
    openp = base * (1 + 0.0005*i)
    high = openp * (1.012 if ts.hour == 10 and ts.day in [21,22] else 1.003)
    low = openp * 0.997
    close = openp * 1.001
    rows.append([ts, openp, high, low, close, 10+i, 0])
df=pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","value"])
Path("data/sample").mkdir(parents=True, exist_ok=True)
df.to_csv("data/sample/upbit_hourly.csv", index=False, encoding="utf-8-sig")
print("saved data/sample/upbit_hourly.csv")
