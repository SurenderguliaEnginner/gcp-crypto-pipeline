import json
import logging
import os

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-prices"
KAFKA_GROUP_ID = "crypto-db-writer"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection
connection = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)
connection.autocommit = True
cursor = connection.cursor()

# Table banao (agar pehle se hai to kuch nahi hoga)
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS crypto_prices (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        price NUMERIC NOT NULL,
        currency TEXT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        inserted_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
)

# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

logger.info("Crypto consumer started")

try:
    for message in consumer:
        data = message.value

        cursor.execute(
            """
            INSERT INTO crypto_prices (symbol, price, currency, event_time)
            VALUES (%s, %s, %s, %s)
            """,
            (data["symbol"], data["price"], data["currency"], data["timestamp"]),
        )

        logger.info(
            "Saved | offset=%s | %s=%s",
            message.offset,
            data["symbol"],
            data["price"],
        )

except KeyboardInterrupt:
    logger.info("Consumer stopped by user")

finally:
    consumer.close()
    cursor.close()
    connection.close()
    logger.info("Consumer closed")