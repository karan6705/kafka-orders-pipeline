import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

# Producer
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", 0.5))

# Consumer
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "orders-consumer-group")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data/output")

# AWS (optional)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/orders/")
