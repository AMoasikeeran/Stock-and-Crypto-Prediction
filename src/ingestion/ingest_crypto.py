"""
Crypto data ingestion module.

Fetches historical cryptocurrency price data from CoinGecko
and stores it in a clean tabular format for downstream processing.
"""

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path


# =========================
# Configuration
# =========================

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
DEFAULT_VS_CURRENCY = "usd"
DEFAULT_DAYS = "max"  # full history

DATA_DIR = Path("data/crypto")
DATA_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# Core Functions
# =========================

def fetch_crypto_market_data(
    coin_id: str,
    vs_currency: str = DEFAULT_VS_CURRENCY,
    days: str = DEFAULT_DAYS
) -> dict:
    """
    Fetch historical market data for a given cryptocurrency.

    Args:
        coin_id (str): CoinGecko coin id (e.g. 'bitcoin', 'ethereum')
        vs_currency (str): Quote currency (default: usd)
        days (str): Number of days ('max' for full history)

    Returns:
        dict: Raw JSON response from CoinGecko API
    """
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart"

    params = {
        "vs_currency": vs_currency,
        "days": days
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    return response.json()


def transform_market_data(raw_data: dict) -> pd.DataFrame:
    """
    Transform raw CoinGecko market data into a clean DataFrame.

    Args:
        raw_data (dict): Raw API response

    Returns:
        pd.DataFrame: Cleaned OHLC-style dataset
    """
    prices = pd.DataFrame(raw_data["prices"], columns=["timestamp", "price"])
    volumes = pd.DataFrame(raw_data["total_volumes"], columns=["timestamp", "volume"])

    df = prices.merge(volumes, on="timestamp")

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def save_crypto_data(df: pd.DataFrame, coin_id: str) -> Path:
    """
    Save transformed crypto data to CSV.

    Args:
        df (pd.DataFrame): Clean crypto dataset
        coin_id (str): Coin identifier

    Returns:
        Path: Path to saved file
    """
    file_path = DATA_DIR / f"{coin_id}_historical.csv"
    df.to_csv(file_path, index=False)

    return file_path


# =========================
# Orchestration
# =========================

def run_crypto_ingestion(coin_id: str) -> None:
    """
    Run full ingestion pipeline for a cryptocurrency.

    Args:
        coin_id (str): CoinGecko coin id
    """
    print(f"[INFO] Starting ingestion for crypto: {coin_id}")

    raw_data = fetch_crypto_market_data(coin_id)
    df = transform_market_data(raw_data)
    output_path = save_crypto_data(df, coin_id)

    print(f"[SUCCESS] Data saved to {output_path}")
    print(f"[INFO] Rows ingested: {len(df)}")


# =========================
# Entry Point
# =========================

if __name__ == "__main__":
    # Example cryptos
    run_crypto_ingestion("bitcoin")
    run_crypto_ingestion("ethereum")