import json
import random
import uuid
import time
import ssl
import certifi
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Event Hub Configuration
EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"
EVENT_HUB_NAME="<<EVENT_HUB_NAME>>"  
CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"

# SSL Context
ssl_context = ssl.create_default_context(cafile=certifi.where())

producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    ssl_context=ssl_context
)

# Reference Data
process_stages = ["Intake", "Validation", "Processing", "Completion"]
event_statuses = ["InProgress", "Completed", "Failed"]
priorities = ["Low", "Medium", "High", "Critical"]
source_systems = ["ERP", "CRM", "OMS", "Monitoring"]
regions = ["EU", "US", "APAC"]

# Dirty Data Injection
def inject_dirty_data(event):

    # 5% future start time
    if random.random() < 0.05:
        event["event_start_time"] = (
            datetime.utcnow() + timedelta(hours=random.randint(1, 24))
        ).isoformat()

    # 5% negative processing time
    if random.random() < 0.05:
        event["processing_time_sec"] = random.randint(-500, -1)

    # 3% missing end time
    if random.random() < 0.03:
        event["event_end_time"] = None

    return event


# Event Generator
def generate_operational_event():

    start_time = datetime.utcnow() - timedelta(minutes=random.randint(1, 180))
    processing_time = random.randint(10, 1800)
    end_time = start_time + timedelta(seconds=processing_time)

    event = {
        "event_id": str(uuid.uuid4()),
        "process_id": f"PROC-{random.randint(1000, 9999)}",
        "process_stage": random.choice(process_stages),
        "event_status": random.choice(event_statuses),
        "priority": random.choice(priorities),
        "source_system": random.choice(source_systems),
        "region": random.choice(regions),
        "event_start_time": start_time.isoformat(),
        "event_end_time": end_time.isoformat(),
        "processing_time_sec": processing_time
    }

    return inject_dirty_data(event)

# Streaming Loop
if __name__ == "__main__":
    while True:
        event = generate_operational_event()
        producer.send(EVENT_HUB_NAME, event)
        print(f"Sent event: {event}")
        time.sleep(1)