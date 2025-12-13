# 📈 Stock & Crypto Prediction Platform

An **end-to-end fintech data platform** that ingests real stock and cryptocurrency market data, stores it in a cloud-based data lake, processes it using modern data engineering tools, and trains machine learning models to predict future price movements and generate trading signals.

This project is designed as a **learning-focused yet industry-aligned portfolio project**, simulating how real-world trading, quantitative, and fintech platforms handle data ingestion, processing, and predictive modelling.

---

## 🔍 What This Project Does

The platform performs the following key functions:

### 📊 Market Data Ingestion
- Collects **historical and near real-time price data** for:
  - Selected **stocks** (e.g. AAPL, MSFT, TSLA)
  - Selected **crypto trading pairs** (e.g. BTC/USDT, ETH/USDT)
- Uses **reliable public market data sources** with minimal setup friction.
- Implements robust ingestion patterns (pagination, retries, logging).

### 🗄️ Data Storage
- Stores **raw market data** in a data lake:
  - Local filesystem (development)
  - **AWS S3 (Free Tier)** for cloud storage
- Organises data into raw, processed, and model-ready layers.

### 🧹 Data Processing & Feature Engineering
- Cleans and transforms raw market data into **feature-rich time series**.
- Generates features such as:
  - Returns
  - Rolling averages
  - Volatility metrics
  - Momentum indicators

### 🤖 Machine Learning & Prediction
- Trains ML models to predict:
  - Next-period price or return
  - Directional movement (up / down)
- Generates **trading-style signals**:
  - BUY / SELL / HOLD
- Evaluates model performance using standard metrics.

---

## 🧱 Tech Stack (Planned & In Progress)

### Languages
- Python
- SQL

### Data & Storage
- CSV / Parquet
- **AWS S3 (Free Tier)**
- PostgreSQL (via Docker)

### Processing & Machine Learning
- Pandas / Polars
- scikit-learn
- *(Optional extension)* PyTorch / TensorFlow (e.g. LSTM models)
- *(Later stage)* Apache Spark via **Databricks Free Edition**

### Orchestration
- Apache Airflow (Docker-based)

### Infrastructure & Tooling
- Docker & docker-compose
- Git & GitHub
- Jira or GitHub Projects for task tracking

⚠️ **Note:**  
This project is intentionally built using **free-tier and open-source tools only**, ensuring it is fully reproducible without paid services.

---

## 🗂 Repository Structure

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