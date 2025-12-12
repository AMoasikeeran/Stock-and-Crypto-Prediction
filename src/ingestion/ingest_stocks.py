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
# Stock-specific settings (separate from commodity)
STOCK_FUNCTION = os.getenv("ALPHAVANTAGE_STOCK_FUNCTION", "TIME_SERIES_DAILY")
STOCK_OUTPUT_SIZE = (
    os.getenv("ALPHAVANTAGE_STOCK_OUTPUTSIZE")
    or os.getenv("ALPHAVANTAGE_OUTPUTSIZE")
    or "compact"
)
# Used by commodity endpoints such as NATURAL_GAS
API_INTERVAL = os.getenv("ALPHAVANTAGE_INTERVAL", "monthly")
# Allow separate keys / fallback for commodities (demo key helps when hitting limits)
COMMODITY_API_KEY = os.getenv("ALPHAVANTAGE_COMMODITY_API_KEY", API_KEY)
COMMODITY_FALLBACK_KEY = os.getenv("ALPHAVANTAGE_COMMODITY_FALLBACK_KEY", "demo")
BRENT_API_KEY = os.getenv("ALPHAVANTAGE_BRENT_API_KEY", COMMODITY_API_KEY)
NAT_GAS_API_KEY = os.getenv("ALPHAVANTAGE_NATURAL_GAS_API_KEY", COMMODITY_API_KEY)
MAX_RETRIES = int(os.getenv("ALPHAVANTAGE_MAX_RETRIES", "2"))
RETRY_DELAY_SECONDS = int(os.getenv("ALPHAVANTAGE_RETRY_DELAY_SECONDS", "60"))

DATA_DIR = Path("data/raw/stocks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

STOCK_SYMBOLS = ["AAPL", "MSFT", "TSLA", "IBM"]


def fetch_daily_stock(
    symbol: str, function: str, output_size: str, api_key: str = API_KEY
) -> pd.DataFrame:
    """Fetch daily OHLCV data for one stock."""
    current_output_size = output_size
    for attempt in range(1, 3):  # at most one fallback attempt (full -> compact)
        params = {
            "function": function,
            "symbol": symbol,
            "outputsize": current_output_size,
            "datatype": "json",
            "apikey": api_key,
        }
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        info_msg = data.get("Information") or data.get("Note")
        if info_msg:
            premium_full = "outputsize=full" in info_msg.lower() or "premium feature" in info_msg.lower()
            if premium_full and current_output_size.lower() == "full":
                print(
                    f"Alpha Vantage message for {symbol}: {info_msg} "
                    "Retrying with outputsize=compact..."
                )
                current_output_size = "compact"
                continue

            raise ValueError(
                f"Alpha Vantage returned a message for {symbol}: {info_msg} "
                f"(function={function}, outputsize={current_output_size}). "
                "Set ALPHAVANTAGE_STOCK_OUTPUTSIZE=compact in your .env to avoid premium limits."
            )
        break

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


def fetch_commodity(
    function_name: str, interval: str, symbol_label: str, api_key: str
) -> pd.DataFrame:
    """Fetch commodity prices from Alpha Vantage (e.g., NATURAL_GAS, BRENT)."""
    key_to_use = api_key
    fallback_used = False

    for attempt in range(1, MAX_RETRIES + 1):
        params = {
            "function": function_name,
            "interval": interval,
            "datatype": "json",
            "apikey": key_to_use,
        }
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        info_msg = data.get("Information") or data.get("Note")
        if info_msg:
            # Try demo fallback once if the primary key hit rate limits
            if (
                not fallback_used
                and COMMODITY_FALLBACK_KEY
                and key_to_use != COMMODITY_FALLBACK_KEY
            ):
                print(
                    f"Alpha Vantage message for {function_name}: {info_msg}. "
                    "Retrying with fallback key..."
                )
                key_to_use = COMMODITY_FALLBACK_KEY
                fallback_used = True
                continue

            if attempt < MAX_RETRIES:
                print(
                    f"Alpha Vantage message for {function_name}: {info_msg}. "
                    f"Waiting {RETRY_DELAY_SECONDS}s then retrying "
                    f"({attempt}/{MAX_RETRIES})..."
                )
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            raise ValueError(
                f"Alpha Vantage returned a message for {function_name}: {info_msg} "
                f"(interval={interval}). Try waiting a minute or set "
                "ALPHAVANTAGE_COMMODITY_FALLBACK_KEY=demo in your .env."
            )

        if "data" not in data:
            raise ValueError(f"Unexpected response for {function_name}: {data}")

        records = data["data"]
        if not records:
            raise ValueError(f"{function_name} returned no data.")

        df = pd.DataFrame(records)
        df.rename(columns={"value": "price"}, inplace=True)
        df["symbol"] = symbol_label
        df["date"] = pd.to_datetime(df["date"])
        df["price"] = df["price"].astype(float)
        df = df[["symbol", "date", "price"]]
        df.sort_values("date", inplace=True)
        return df

    raise RuntimeError(f"Failed to fetch {function_name} after {MAX_RETRIES} attempts.")


def fetch_natural_gas(interval: str) -> pd.DataFrame:
    return fetch_commodity("NATURAL_GAS", interval, "NATURAL_GAS", NAT_GAS_API_KEY)


def fetch_brent(interval: str) -> pd.DataFrame:
    return fetch_commodity("BRENT", interval, "BRENT", BRENT_API_KEY)


def main():
    if API_FUNCTION.upper() == "NATURAL_GAS":
        print(f"Fetching NATURAL_GAS (interval={API_INTERVAL})...")
        df = fetch_natural_gas(API_INTERVAL)
        out_path = DATA_DIR / f"natural_gas_{API_INTERVAL}.csv"
        df.to_csv(out_path, index=False)
        print(f"Saved {len(df)} rows to {out_path}")
        # Pause briefly to respect rate limits before the next commodity call
        time.sleep(15)
        print(f"Fetching BRENT (interval={API_INTERVAL})...")
        brent_df = fetch_brent(API_INTERVAL)
        brent_path = DATA_DIR / f"brent_{API_INTERVAL}.csv"
        brent_df.to_csv(brent_path, index=False)
        print(f"Saved {len(brent_df)} rows to {brent_path}")
        # Pause before stock call to respect rate limits
        time.sleep(15)
        print(f"Fetching IBM stock (function={STOCK_FUNCTION}, outputsize={STOCK_OUTPUT_SIZE})...")
        ibm_df = fetch_daily_stock("IBM", STOCK_FUNCTION, STOCK_OUTPUT_SIZE)
        ibm_path = DATA_DIR / "IBM_daily.csv"
        ibm_df.to_csv(ibm_path, index=False)
        print(f"Saved {len(ibm_df)} rows to {ibm_path}")
    elif API_FUNCTION.upper() == "BRENT":
        print(f"Fetching BRENT (interval={API_INTERVAL})...")
        brent_df = fetch_brent(API_INTERVAL)
        brent_path = DATA_DIR / f"brent_{API_INTERVAL}.csv"
        brent_df.to_csv(brent_path, index=False)
        print(f"Saved {len(brent_df)} rows to {brent_path}")
    else:
        for symbol in STOCK_SYMBOLS:
            print(f"Fetching {symbol}...")
            df = fetch_daily_stock(symbol, STOCK_FUNCTION, STOCK_OUTPUT_SIZE)
            out_path = DATA_DIR / f"{symbol}_daily.csv"
            df.to_csv(out_path, index=False)
            print(f"Saved {len(df)} rows to {out_path}")
            # Respect free tier limits (5 calls/min); sleep a bit between requests
            time.sleep(15)


if __name__ == "__main__":
    main()
