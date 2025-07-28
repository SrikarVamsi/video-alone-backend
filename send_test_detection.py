import json
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send detection that should trigger alert
detection = {
    "camera_id": "cam_001",
    "object_type": "person",
    "confidence": 0.85,
    "bbox": [100, 200, 300, 400],
    "timestamp": "2025-07-17T10:30:00Z",
    "zone": "restricted_area"
}

producer.send('detection_events', detection)
producer.flush()
print("✅ Sent detection event")
