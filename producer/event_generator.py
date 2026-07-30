"""Simulateur d'evenements de transaction publies dans Kafka."""

import json
import logging
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker("fr_FR")
CATEGORIES = ["electronique", "mode", "maison", "sport", "alimentation"]
TOPIC = "transactions"


def generate_event() -> dict:
    return {
        "transaction_id": fake.uuid4(),
        "amount": round(random.uniform(5, 500), 2),
        "category": random.choice(CATEGORIES),
        "city": fake.city(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main(bootstrap_servers: str = "localhost:9092", interval_seconds: float = 0.5) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info("Demarrage du producteur d'evenements sur le topic %s", TOPIC)

    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            logger.info("Evenement envoye: %s", event)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Arret du producteur")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
