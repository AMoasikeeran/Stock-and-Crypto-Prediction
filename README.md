# Stock-and-Crypto-Prediction

An end-to-end **fintech data platform** that ingests **real stock and crypto market data**, stores it in a **cloud data lake (AWS S3)**, processes it with **Python / Spark**, and trains **machine learning models** to predict future price movements and generate **trading signals**.

## 🔍 What this project does

- Collects **real-time & historical** price data for:
  - Selected **stocks** (e.g. AAPL, MSFT, TSLA).
  - Selected **crypto pairs** (e.g. BTC/USDT, ETH/USDT).
- Stores raw market data in a **data lake** (local + AWS S3).
- Cleans and transforms data into **feature-rich time series**
- Trains **ML models** to predict:
  - Next price/return.
  - Up/down movement.
- Outputs **signals** (BUY / SELL / HOLD) and evaluation metrics.

This project is built as a **learning and portfolio project** to simulate how real trading/quant/fintech platforms handle data and predictions.

## 🧱 Tech Stack (Planned)

- **Languages:** Python, SQL
- **Data & Storage:** Parquet/CSV, AWS S3 (free tier), PostgreSQL (Docker)
- **Processing & ML:**
  - Pandas / Polars
  - (Later) Spark on **Databricks Free Edition**
- **Orchestration:** Apache Airflow (Docker)
- **Modelling:** scikit-learn, (optionally PyTorch / TensorFlow for LSTMs)
- **Infrastructure / Tools:**
  - Docker & docker-compose
  - Git + GitHub
  - Jira or GitHub Projects for task tracking

⚠️ This project is designed to utilise **free tiers only** (AWS S3 free tier, Databricks Free Edition, and free stock/crypto APIs).

## 🗂 Repository Structure (initial draft)

```text
stock-crypto-prediction-platform/
├── README.md
├── requirements.txt
├── src/
│   ├── ingestion/
│   │   ├── fetch_stocks_alpha_vantage.py
│   │   └── fetch_crypto_binance.py
│   ├── features/
│   ├── models/
│   └── utils/
├── notebooks/
├── airflow/
│   └── dags/
├── infra/
│   └── s3/
└── data/
    ├── raw/
    ├── processed/
    └── models/
