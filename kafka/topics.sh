#!/bin/bash
docker exec -it kafka-kafka-1 kafka-topics \
  --create --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1