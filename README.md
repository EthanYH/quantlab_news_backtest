# QuantLab News Sentiment Backtest

뉴스 감성 점수와 업비트 시간봉 데이터를 이용해 다음 규칙을 백테스트합니다.

- 전일/당일 08:50 이전 뉴스 기준으로 매수 여부 결정
- 긍정 신호가 기준 이상이면 08:50 이후 첫 시간봉 시가로 진입
- 24시간 이내 +1% 도달 시 매도
- 손절, 수수료, 최소 기사 수 조건 포함

## 설치

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 샘플 실행

```bash
python scripts/make_sample_price.py
python scripts/run_backtest.py --price data/sample/upbit_hourly.csv --news data/sample/news.csv
```

결과는 `reports/`에 저장됩니다.

- `news_scored.csv`: 뉴스별 감성 점수
- `daily_features.csv`: 일별 뉴스 feature
- `relation_dataset.csv`: 뉴스 feature와 당일 가격 결과 병합 데이터
- `trades.csv`: 백테스트 거래 내역
- `stats.json`: 승률, 총수익률, MDD 등

## 실제 데이터 수집

업비트 시간봉:

```bash
python scripts/collect_upbit.py --config config.yaml --out data/raw/upbit_hourly.csv
```

뉴스 샘플 수집(Google News RSS):

```bash
python scripts/collect_news.py --source google_rss --out data/raw/news.csv
```

US news + Reddit + Twitter/X:

```bash
# Google US news + Reddit. Twitter/X needs a bearer token, so this skips it if missing.
python scripts/collect_news.py --source google_rss,reddit --out data/raw/news.csv

# Google US news + Reddit + Twitter/X.
set TWITTER_BEARER_TOKEN=...
python scripts/collect_news.py --source all --out data/raw/news.csv

# Keep partial results when one source fails.
python scripts/collect_news.py --source all --keep-going --out data/raw/news.csv
```

Source settings live in `config.yaml` under `google_news`, `reddit`, and `twitter`.

## Daily Telegram Signal

Set Telegram credentials:

```bash
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."
```

Test without sending:

```bash
python scripts/send_daily_telegram.py --dry-run
```

Send once:

```bash
python scripts/send_daily_telegram.py
```

Run every day at 08:50 on Raspberry Pi:

```bash
mkdir -p logs
crontab -e
```

Add one line:

```cron
50 8 * * * cd /home/yonghan1205/quantlab_news_backtest && . .venv/bin/activate && TELEGRAM_BOT_TOKEN="..." TELEGRAM_CHAT_ID="..." python scripts/send_daily_telegram.py >> logs/daily_telegram.log 2>&1
```

네이버 뉴스 API 사용 시:

```bash
export NAVER_CLIENT_ID="..."
export NAVER_CLIENT_SECRET="..."
python scripts/collect_news.py --source naver --out data/raw/news.csv
```

백테스트:

```bash
python scripts/run_backtest.py --price data/raw/upbit_hourly.csv --news data/raw/news.csv
```

## 튜닝 포인트

`config.yaml`에서 아래 값을 바꿔가며 테스트합니다.

- `sentiment_threshold`: 진입 감성 기준
- `min_article_count`: 최소 기사 수
- `take_profit_pct`: 익절 기준, 기본 1%
- `stop_loss_pct`: 손절 기준
- `fee_rate`: 업비트 수수료 가정

## 주의

이 코드는 투자 권유가 아니라 연구/백테스트용입니다. 실거래 전에는 긴 기간의 데이터, 슬리피지, API 지연, 중복 뉴스 제거, 생존편향을 반드시 검증해야 합니다.
