import os
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")  # put this in a .env file
BASE_URL = "https://www.alphavantage.co/query"

DATA_DIR = Path("data/raw/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

STOCK_SYMBOLS = ["AAPL", "MSFT", "TSLA"]


def fetch_daily_stock(symbol: str) -> pd.DataFrame:
    """Fetch daily OHLCV data for one stock."""
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "full",
        "datatype": "json",
        "apikey": API_KEY,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Alpha Vantage returns nested JSON; "Time Series (Daily)" contains date→values
    key = "Time Series (Daily)"
    if key not in data:
        raise ValueError(f"Unexpected response for {symbol}: {data}")

    records = []
    for date, values in data[key].items():
        records.append(
            {
                "symbol": symbol,
                "date": date,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "adjusted_close": float(values["5. adjusted close"]),
                "volume": int(values["6. volume"]),
            }
        )

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    return df


def main():
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
