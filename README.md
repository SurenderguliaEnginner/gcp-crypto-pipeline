# Crypto Market Data Pipeline

An end-to-end data engineering pipeline that streams live cryptocurrency prices from the CoinGecko API, processes them through Kafka, stores them in a cloud database, and transforms the data using a Bronze → Silver → Gold architecture — fully automated with Apache Airflow and Databricks.

## Architecture


## Tech Stack

- **Ingestion:** Python, CoinGecko API
- **Streaming:** Apache Kafka (Docker)
- **Storage:** Google Cloud SQL (PostgreSQL)
- **Orchestration:** Apache Airflow (Docker, TaskFlow API)
- **Processing:** Databricks (PySpark, Delta Lake)
- **Infrastructure:** Google Cloud Platform (Compute Engine VM)

## How It Works

1. **Producer** fetches live Bitcoin & Ethereum prices from CoinGecko and publishes them to a Kafka topic.
2. **Consumer** reads messages from Kafka and writes them into a PostgreSQL table on Cloud SQL.
3. **Airflow** runs this entire flow automatically every 5 minutes, then triggers a Databricks Job.
4. **Databricks** reads the raw data (Bronze), cleans and deduplicates it (Silver), and aggregates hourly price summaries (Gold) using Delta Lake tables.

## Project Structure


## Key Learnings

- Building a real-time streaming pipeline with Kafka
- Orchestrating multi-step workflows with Airflow's TaskFlow API
- Implementing the Bronze/Silver/Gold data lakehouse pattern in Databricks
- Managing secrets with Airflow Variables instead of hardcoding credentials
- Debugging Docker networking between independently-managed containers
- Working with Google Cloud Platform (Compute Engine, Cloud SQL, IAM, firewall rules, quotas)

## Future Improvements

- Move secrets to a dedicated secrets manager (e.g. GCP Secret Manager)
- Restrict database network access instead of allowing all IPs
- Add data quality checks and alerting on pipeline failures
- Build a dashboard on top of the Gold layer for visualization