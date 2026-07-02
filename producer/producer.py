import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# --- Config from .env ---
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_API_KEY = os.environ["KAFKA_API_KEY"]
KAFKA_API_SECRET = os.environ["KAFKA_API_SECRET"]
TOPIC = os.environ.get("KAFKA_TOPIC", "inventory-events")

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": KAFKA_API_KEY,
    "sasl.password": KAFKA_API_SECRET,
}

producer = Producer(conf)

# --- Load reference products ---
PRODUCTS = []
with open("ref_products.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        PRODUCTS.append({
            "product_id": row["StockCode"],
            "description": row["description"],
            "unit_price": float(row["unit_price"]),
        })

# --- Load reference warehouses ---
WAREHOUSES = []
with open("ref_warehouses.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        WAREHOUSES.append({
            "warehouse_id": row["warehouse_id"],
            "country": row["country"],
        })

MOVEMENT_TYPES = ["SALE", "RESTOCK", "RETURN", "TRANSFER", "ADJUSTMENT"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered event to topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def make_inventory_movement():
    product = random.choice(PRODUCTS)
    warehouse = random.choice(WAREHOUSES)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "inventory_movement",
        "product_id": product["product_id"],
        "product_description": product["description"],
        "warehouse_id": warehouse["warehouse_id"],
        "warehouse_country": warehouse["country"],
        "movement_type": random.choice(MOVEMENT_TYPES),
        "quantity_change": random.randint(-50, 100),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_price_update():
    product = random.choice(PRODUCTS)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "price_update",
        "product_id": product["product_id"],
        "product_description": product["description"],
        "old_price": product["unit_price"],
        "new_price": round(random.uniform(0.50, 100.00), 2),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_bad_record():
    """Deliberately malformed records for data-quality testing."""
    choice = random.randint(1, 3)

    if choice == 1:
        event = make_inventory_movement()
        event.pop("product_id")
        return event

    if choice == 2:
        event = make_inventory_movement()
        event["movement_type"] = "RESTOCK"
        event["quantity_change"] = -999
        return event

    event = make_inventory_movement()
    event["event_timestamp"] = (
        datetime.now(timezone.utc) - timedelta(hours=2)
    ).isoformat()
    return event


def pick_event():
    r = random.random()

    if r < 0.70:
        return make_inventory_movement()
    elif r < 0.90:
        return make_price_update()
    else:
        return make_bad_record()


if __name__ == "__main__":
    print(f"Producing to topic: {TOPIC}")
    print(f"Bootstrap server: {KAFKA_BOOTSTRAP}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            event = pick_event()
            key = event.get("product_id", "unknown")

            producer.produce(
                TOPIC,
                key=key,
                value=json.dumps(event),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("Producer stopped.")
