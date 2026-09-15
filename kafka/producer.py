import json
import os
import time

import psycopg2
from kafka import KafkaProducer

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "market_analysis"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDERS_TOPIC", "instacart.orders")


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, user_id, eval_set, order_number,
                       order_dow, order_hour_of_day, days_since_prior_order
                FROM orders
                ORDER BY order_id;
                """
            )

            for row in cur:
                event = {
                    "order_id": row[0],
                    "user_id": row[1],
                    "eval_set": row[2],
                    "order_number": row[3],
                    "order_dow": row[4],
                    "order_hour_of_day": row[5],
                    "days_since_prior_order": (
                        float(row[6]) if row[6] is not None else None
                    ),
                }

                producer.send(TOPIC, value=event)
                print(f"Published order {event['order_id']}")
                time.sleep(0.01)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
