import boto3
import pandas as pd
import json
import time

df = pd.read_csv("data/vehicle_data.csv")
client = boto3.client("kinesis", region_name="ap-southeast-2")

for i, (_, row) in enumerate(df.iterrows()):
    record = row.to_json()
    client.put_record(StreamName="vehicle-stream", Data=record, PartitionKey="vehicle")
    print(f"Sent record {i + 1}")
    time.sleep(0.2)
