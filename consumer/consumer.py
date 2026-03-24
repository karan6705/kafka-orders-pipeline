import json
import sys
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    CONSUMER_GROUP_ID,
    OUTPUT_DIR,
    S3_BUCKET_NAME,
)
from utils.logger import get_logger

logger = get_logger("consumer")


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,  # stop after 10s of no messages
    )


def get_output_path(output_dir: str) -> Path:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    return Path(output_dir) / f"orders_{date_str}.jsonl"


def write_record(record: dict, output_path: Path) -> None:
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def upload_to_s3(output_path: Path) -> None:
    """
    Upload the local output file to S3.
    Requires AWS credentials set in .env or environment.
    """
    try:
        import boto3
        from config.settings import AWS_REGION, S3_PREFIX

        s3_key = f"{S3_PREFIX}{output_path.name}"
        client = boto3.client("s3", region_name=AWS_REGION)
        client.upload_file(str(output_path), S3_BUCKET_NAME, s3_key)
        logger.info(f"Uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")


def main():
    start_time = time.time()
    logger.info(f"Starting consumer | broker={KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC} | group={CONSUMER_GROUP_ID}")

    try:
        consumer = build_consumer()
    except KafkaError as e:
        logger.error(f"Could not connect to Kafka: {e}")
        sys.exit(1)

    output_path = get_output_path(OUTPUT_DIR)
    logger.info(f"Writing records to: {output_path}")

    message_count = 0

    try:
        for message in consumer:
            record = message.value
            write_record(record, output_path)
            message_count += 1
            logger.info(
                f"Consumed message {message_count}: "
                f"order_id={record.get('order_id')} | "
                f"status={record.get('status')} | "
                f"total=${record.get('total_price')}"
            )
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        consumer.close()
        duration = round(time.time() - start_time, 2)
        logger.info(f"Session complete | messages={message_count} | output={output_path} | time={duration}s")

        # Optional: upload to S3 after session ends
        if S3_BUCKET_NAME and message_count > 0:
            logger.info("S3_BUCKET_NAME detected. Uploading output file to S3...")
            upload_to_s3(output_path)


if __name__ == "__main__":
    main()
