import argparse
import csv
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_DELAY_SECONDS,
)
from utils.logger import get_logger

logger = get_logger("producer")

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "input" / "orders_sample.csv"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def on_send_success(record_metadata):
    logger.info(
        f"Delivered → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc):
    logger.error(f"Failed to deliver message: {exc}")


def stream_orders(producer: KafkaProducer, csv_path: Path) -> None:
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            # Cast numeric fields
            row["quantity"] = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
            row["total_price"] = float(row["total_price"])

            producer.send(KAFKA_TOPIC, value=row).add_callback(
                on_send_success
            ).add_errback(on_send_error)

            logger.info(f"Sent message {i}: order_id={row['order_id']} | customer={row['customer_id']}")

            time.sleep(PRODUCER_DELAY_SECONDS)

    producer.flush()
    logger.info("All messages sent. Producer shutting down.")


def main():
    parser = argparse.ArgumentParser(description="Kafka Orders Producer")
    parser.add_argument("--loop", action="store_true", help="Continuously loop the dataset")
    args = parser.parse_args()

    logger.info(f"Starting producer | broker={KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC}")
    logger.info(f"Streaming delay: {PRODUCER_DELAY_SECONDS}s per message")

    try:
        producer = build_producer()
    except KafkaError as e:
        logger.error(f"Could not connect to Kafka: {e}")
        sys.exit(1)

    try:
        while True:
            stream_orders(producer, CSV_PATH)
            if not args.loop:
                break
            logger.info("Replaying dataset...")
            time.sleep(2)  # Short pause before starting over
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Flushing and closing producer.")
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
