from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Reading first 5 messages:")
for i, msg in enumerate(consumer):
    print(msg.value)
    if i >= 4:
        break