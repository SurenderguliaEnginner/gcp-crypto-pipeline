import json
import logging
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer


# ---------- Configuration ----------

API_URL = "https://api.coingecko.com/api/v3/simple/price"

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "crypto-prices"

COINS = ["bitcoin", "ethereum"]
CURRENCY = "usd"

REQUEST_TIMEOUT = 10
PUBLISH_INTERVAL = 5


# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------- API Client ----------

def fetch_prices() -> dict:
    """Fetch current crypto prices from CoinGecko."""

    params = {
        "ids": ",".join(COINS),
        "vs_currencies": CURRENCY,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    return response.json()


# ---------- Kafka Producer ----------

def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )


def build_messages(prices: dict) -> list[dict]:
    """Convert API response into Kafka messages."""

    timestamp = datetime.now(timezone.utc).isoformat()

    messages = []

    for coin in COINS:
        price = prices.get(coin, {}).get(CURRENCY)

        if price is None:
            logger.warning("Missing price for %s", coin)
            continue

        messages.append(
            {
                "symbol": coin,
                "price": price,
                "currency": CURRENCY,
                "timestamp": timestamp,
            }
        )

    return messages


# ---------- Main Pipeline ----------

def main() -> None:
    producer = create_kafka_producer()

    logger.info("Crypto producer started")

    try:
        while True:
            try:
                prices = fetch_prices()

                messages = build_messages(prices)

                for message in messages:
                    future = producer.send(
                        KAFKA_TOPIC,
                        value=message,
                        key=message["symbol"].encode("utf-8"),
                    )

                    metadata = future.get(timeout=10)

                    logger.info(
                        "Published %s=$%s | partition=%s | offset=%s",
                        message["symbol"],
                        message["price"],
                        metadata.partition,
                        metadata.offset,
                    )

                producer.flush()

            except requests.RequestException as exc:
                logger.error("CoinGecko API request failed: %s", exc)

            except Exception:
                logger.exception("Unexpected producer error")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped")


if __name__ == "__main__":
    main()