import json
import time
import random
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# editing
EVENT_TYPES = ["page_view", "search", "add_to_cart", "purchase", "logout"]
DEVICES = ["mobile", "desktop", "tablet"]

def generate_event():
    return {
        "user_id": fake.uuid4(),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": fake.date_time_this_year().isoformat(),
        "session_id": fake.uuid4(),
        "device": random.choice(DEVICES),
        "page": fake.uri_path(),
        "country": fake.country_code(),
        "amount": round(random.uniform(5, 500), 2) if random.random() > 0.7 else None
    }

if __name__ == "__main__":
    print("Starting producer... sending 500,000 events")
    for i in range(500_000):
        event = generate_event()
        producer.send('user-events', value=event)
        if i % 10_000 == 0:
            print(f"Sent {i} events")
    producer.flush()
    print("Done.")