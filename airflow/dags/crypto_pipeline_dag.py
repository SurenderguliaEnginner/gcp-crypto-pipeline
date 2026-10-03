from datetime import datetime, timedelta

import requests
import json
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

from airflow.sdk import dag, task
from airflow.models import Variable

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "crypto-prices"

DB_HOST = "34.93.124.156"
DB_PORT = "5432"
DB_NAME = "market_pipeline"
DB_USER = "crypto_user"


@dag(
    dag_id="crypto_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crypto"],
)
def crypto_pipeline():

    @task
    def fetch_and_publish():
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        timestamp = datetime.utcnow().isoformat() + "+00:00"

        for symbol, prices in data.items():
            message = {
                "symbol": symbol,
                "price": prices["usd"],
                "currency": "usd",
                "timestamp": timestamp,
            }
            producer.send(KAFKA_TOPIC, message)

        producer.flush()
        producer.close()

    @task
    def consume_and_save():
        db_password = Variable.get("db_password")

        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=db_password,
            sslmode="require",
        )
        connection.autocommit = True
        cursor = connection.cursor()

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="airflow-crypto-writer",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=8000,
        )

        for message in consumer:
            data = message.value
            cursor.execute(
                """
                INSERT INTO crypto_prices (symbol, price, currency, event_time)
                VALUES (%s, %s, %s, %s)
                """,
                (data["symbol"], data["price"], data["currency"], data["timestamp"]),
            )

        consumer.close()
        cursor.close()
        connection.close()

    @task
    def trigger_databricks_job():
        databricks_host = Variable.get("databricks_host")
        databricks_token = Variable.get("databricks_token")
        databricks_job_id = Variable.get("databricks_job_id")

        url = f"{databricks_host}/api/2.1/jobs/run-now"
        headers = {"Authorization": f"Bearer {databricks_token}"}
        payload = {"job_id": databricks_job_id}

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print("Databricks job triggered:", response.json())

    fetch_and_publish() >> consume_and_save() >> trigger_databricks_job()


crypto_pipeline()