import json
import os
import pandas as pd
from tqdm import tqdm
from confluent_kafka import Producer

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CSV_PATH = os.getenv("CSV_PATH", "data/train.csv")

def delivery_report(err, msg):
    if err is not None:
        raise RuntimeError(f"Delivery failed: {err}")

def main():
    df = pd.read_csv(CSV_PATH)
    p = Producer({"bootstrap.servers": BOOTSTRAP})

    for _, row in tqdm(df.iterrows(), total=len(df)):
        payload = row.to_dict()
        p.produce(TOPIC, json.dumps(payload).encode("utf-8"), callback=delivery_report)
        p.poll(0)

    p.flush()
    print(f"Done. Sent {len(df)} messages to topic '{TOPIC}'")

if __name__ == "__main__":
    main()
