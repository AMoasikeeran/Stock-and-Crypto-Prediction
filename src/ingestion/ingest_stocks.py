import os
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")  # put this in a .env file
if not API_KEY:
    raise RuntimeError("ALPHAVANTAGE_API_KEY is missing; set it in your .env file.")

BASE_URL = "https://www.alphavantage.co/query"
# Default to the free Natural Gas commodity endpoint the user provided; override via env
API_FUNCTION = os.getenv("ALPHAVANTAGE_FUNCTION", "NATURAL_GAS")
# Used by stock endpoints (ignored by commodity endpoints)
API_OUTPUT_SIZE = os.getenv("ALPHAVANTAGE_OUTPUTSIZE", "full")
# Used by commodity endpoints such as NATURAL_GAS
API_INTERVAL = os.getenv("ALPHAVANTAGE_INTERVAL", "monthly")

DATA_DIR = Path("data/raw/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

STOCK_SYMBOLS = ["AAPL", "MSFT", "TSLA"]


def fetch_daily_stock(symbol: str) -> pd.DataFrame:
    """Fetch daily OHLCV data for one stock."""
    params = {
        "function": API_FUNCTION,
        "symbol": symbol,
        "outputsize": API_OUTPUT_SIZE,
        "datatype": "json",
        "apikey": API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    info_msg = data.get("Information") or data.get("Note")
    if info_msg:
        raise ValueError(
            f"Alpha Vantage returned a message for {symbol}: {info_msg} "
            f"(function={API_FUNCTION}, outputsize={API_OUTPUT_SIZE}). "
            "Set ALPHAVANTAGE_FUNCTION=TIME_SERIES_DAILY and/or "
            "ALPHAVANTAGE_OUTPUTSIZE=compact in your .env to avoid premium limits."
        )

    # Alpha Vantage returns nested JSON; "Time Series (Daily)" contains date→values
    key = "Time Series (Daily)"
    if key not in data:
        raise ValueError(f"Unexpected response for {symbol}: {data}")

    records = []
    for date, values in data[key].items():
        close = float(values["4. close"])
        adjusted_close = float(values.get("5. adjusted close", close))
        volume = values.get("6. volume") or values.get("5. volume")
        if volume is None:
            raise ValueError(f"Volume not found in response for {symbol}: {values}")

        records.append(
            {
                "symbol": symbol,
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": close,
                "adjusted_close": adjusted_close,
                "volume": int(volume),
            }
        )

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    return df


def fetch_natural_gas(interval: str) -> pd.DataFrame:
    """Fetch monthly Natural Gas prices (free Alpha Vantage commodity endpoint)."""
    params = {
        "function": "NATURAL_GAS",
        "interval": interval,
        "datatype": "json",
        "apikey": API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    info_msg = data.get("Information") or data.get("Note")
    if info_msg:
        raise ValueError(
            f"Alpha Vantage returned a message for NATURAL_GAS: {info_msg} "
            f"(interval={interval}). Try waiting a minute or use the demo key "
            "for testing."
        )

    if "data" not in data:
        raise ValueError(f"Unexpected response for NATURAL_GAS: {data}")

    records = data["data"]
    if not records:
        raise ValueError("NATURAL_GAS returned no data.")

    df = pd.DataFrame(records)
    df.rename(columns={"value": "price"}, inplace=True)
    df["symbol"] = "NATURAL_GAS"
    df["date"] = pd.to_datetime(df["date"])
    df["price"] = df["price"].astype(float)
    df = df[["symbol", "date", "price"]]
    df.sort_values("date", inplace=True)
    return df


def main():
    if API_FUNCTION.upper() == "NATURAL_GAS":
        print(f"Fetching NATURAL_GAS (interval={API_INTERVAL})...")
        df = fetch_natural_gas(API_INTERVAL)
        out_path = DATA_DIR / f"natural_gas_{API_INTERVAL}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} rows to {out_path}")
    else:
        for symbol in STOCK_SYMBOLS:
            print(f"Fetching {symbol}...")
            df = fetch_daily_stock(symbol)
            out_path = DATA_DIR / f"{symbol}_daily.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {len(df)} rows to {out_path}")
            # Respect free tier limits (5 calls/min); sleep a bit between requests
            time.sleep(15)


if __name__ == "__main__":
    main()
