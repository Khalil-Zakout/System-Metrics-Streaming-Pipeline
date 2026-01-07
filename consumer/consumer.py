import json
import time
from kafka import KafkaConsumer
from google.cloud import storage
from datetime import datetime, timezone

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "machine_metrics"
PROJECT_ID = "system-monitoring-pipeline"
STORAGE_CLIENT = storage.Client(project= PROJECT_ID)
BUCKET = STORAGE_CLIENT.bucket("machine-metrics-raw")


# Initialize Kafka consumer to read machine metrics
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers = [KAFKA_BOOTSTRAP_SERVER], 
                        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
                        auto_offset_reset = "earliest", enable_auto_commit = True,
                        group_id = "machine_metrics_consumer")


# Function to upload a batch of messages to GCS
def write_to_gcs(batch):
    try:
        # Use UTC timestamp for partitioning
        now = datetime.now(timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%b").upper()
        day = now.strftime("%d")

        # Create blob name with partitions: year/month/day
        blob_name = f"raw-metrics/{year}/{month}/{day}/metrics_{int(now.timestamp())}.json"
        blob = BUCKET.blob(blob_name)

        # Upload JSON batch to GCS
        blob.upload_from_string(json.dumps(batch, indent=2), content_type= "application/json")
        print(f"Uploaded {len(batch)} records to gs://{BUCKET.name}/{blob_name} ✅")

    except Exception as e:
        print(f"❌ Failed to upload batch: {e}")


    

def main():
    batch = []
    last_flush_time = time.time()

    for message in consumer:

        batch.append(message.value)

        now = time.time()
        should_flush = ((len(batch) >= 50) or (now - last_flush_time >= 60))

        if should_flush:
            write_to_gcs(batch)
            batch.clear()
            last_flush_time = now


if __name__ == "__main__":
    main()
            





