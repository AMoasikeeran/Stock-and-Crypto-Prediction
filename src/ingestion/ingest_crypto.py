"""
Crypto data ingestion module (Binance).

Fetches historical OHLCV candlestick data from Binance
and stores it locally for downstream processing.
"""

import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterable

import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuration
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_INTERVAL = "1d"  # daily data (perfect for ML)
DEFAULT_START_DATE = "2017-08-01"
REQUEST_LIMIT = 1000
RATE_LIMIT_SLEEP = 0.2  # seconds

DATA_DIR = Path("data/raw/crypto")
DATA_DIR.mkdir(parents=True, exist_ok=True)

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "num_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]

# =========================
# Helpers
# =========================

def _to_millis(ts: Optional[str | datetime]) -> Optional[int]:
    if ts is None:
        return None
    return int(pd.Timestamp(ts, tz="UTC").timestamp() * 1000)

# =========================
# Core Functions
# =========================

def fetch_binance_klines(
    symbol: str,
    interval: str = DEFAULT_INTERVAL,
    start_date: str | datetime = DEFAULT_START_DATE,
    end_date: Optional[str | datetime] = None,
) -> pd.DataFrame:
    """
    Fetch historical kline (candlestick) data for a symbol from Binance.
    """
    start_ms = _to_millis(start_date)
    end_ms = _to_millis(end_date)
    next_start = start_ms

    frames: list[pd.DataFrame] = []
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "crypto-ingestion/1.0"})

    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": REQUEST_LIMIT,
            "startTime": next_start,
        }
        if end_ms:
            params["endTime"] = end_ms

        resp = session.get(f"{BINANCE_BASE_URL}/klines", params=params, timeout=30)
        resp.raise_for_status()
        klines = resp.json()

        if not klines:
            break

        frame = pd.DataFrame(klines, columns=KLINE_COLUMNS)
        frames.append(frame)

        last_close_time = int(frame["close_time"].iloc[-1])
        next_start = last_close_time + 1

        if (end_ms and next_start >= end_ms) or len(frame) < REQUEST_LIMIT:
            break

        time.sleep(RATE_LIMIT_SLEEP)

    if not frames:
        raise ValueError(f"No data returned for {symbol}")

    df = pd.concat(frames, ignore_index=True)

    # Type conversions
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    return df.sort_values("open_time").reset_index(drop=True)

# =========================
# Persistence
# =========================

def save_crypto_data(df: pd.DataFrame, symbol: str, interval: str) -> Path:
    file_path = DATA_DIR / f"{symbol}_{interval}.csv"
    df.to_csv(file_path, index=False)
    return file_path

# =========================
# Orchestration
# =========================

def run_crypto_ingestion(
    symbols: Iterable[str] = DEFAULT_SYMBOLS,
    interval: str = DEFAULT_INTERVAL,
    start_date: str | datetime = DEFAULT_START_DATE,
    end_date: Optional[str | datetime] = None,
) -> None:
    for symbol in symbols:
        logger.info(f"Ingesting {symbol} ({interval})")
        df = fetch_binance_klines(
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
        path = save_crypto_data(df, symbol, interval)
        logger.info(f"{len(df)} rows saved to {path}")

# =========================
# Entry Point
# =========================

if __name__ == "__main__":
    run_crypto_ingestion()