import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "instacart.orders")
S3_BUCKET = os.getenv("S3_BUCKET", "instacart-market-pipeline")

s3 = boto3.client("s3")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="instacart-s3-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        event = message.value
        now = datetime.now(timezone.utc)
        key = (
            f"raw/orders/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{uuid.uuid4()}.json"
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(event).encode("utf-8"),
            ContentType="application/json",
        )

        print(f"Wrote s3://{S3_BUCKET}/{key}")


if __name__ == "__main__":
    main()
