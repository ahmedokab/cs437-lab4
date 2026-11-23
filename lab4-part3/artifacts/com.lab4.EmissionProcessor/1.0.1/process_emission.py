import json
import math
import threading
import time
import traceback

import boto3
from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2
from awsiot.greengrasscoreipc.model import BinaryMessage, PublishMessage

DATA_TOPIC = "vehicle/+/data"
max_co2 = {}  # vehicle -> highest CO2 seen so far
lock = threading.Lock()  # callbacks run on multiple threads

# ---- Part 3: Firehose ----
FIREHOSE_STREAM_NAME = "lab4data"
AWS_REGION = "us-east-2"
firehose_client = boto3.client("firehose", region_name=AWS_REGION)

ipc = GreengrassCoreIPCClientV2()


def publish_result(thing, result):
    # each car only listens on its own result topic
    ipc.publish_to_topic(
        topic=f"vehicle/{thing}/result",
        publish_message=PublishMessage(
            binary_message=BinaryMessage(message=json.dumps(result).encode())
        ),
    )


def send_to_firehose(thing, row, current_max):
    # JSON can't hold NaN (empty CSV cells), so turn them into null
    clean_row = {}
    for key, value in row.items():
        if isinstance(value, float) and math.isnan(value):
            clean_row[key] = None
        else:
            clean_row[key] = value

    record = {
        "device_id": thing,
        "max_CO2": current_max,
        "data": clean_row,
    }
    firehose_client.put_record(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Record={"Data": json.dumps(record)},
    )


def on_data(event):
    try:
        # bridge sends stuff as binary, but handle json just in case
        if event.binary_message:
            topic = event.binary_message.context.topic
            row = json.loads(event.binary_message.message)
        else:
            topic = event.json_message.context.topic
            row = event.json_message.message

        # get the car from the topic instead of vehicle_id in the payload
        thing = topic.split("/")[1]
        co2 = float(row["vehicle_CO2"])

        with lock:
            is_new_max = co2 > max_co2.get(thing, float("-inf"))
            if is_new_max:
                max_co2[thing] = co2
            current_max = max_co2[thing]

        # Part 3: archive EVERY reading (not just new maxes) for the plots
        send_to_firehose(thing, row, current_max)

        # Part 2: only send back to the car when the max goes up
        if is_new_max:
            result = {
                "vehicle": thing,
                "max_CO2": co2,
                "timestep": row.get("timestep_time"),
            }
            publish_result(thing, result)
            print(f"{thing}: new max CO2 {co2} at t={row.get('timestep_time')}", flush=True)
    except Exception:
        traceback.print_exc()


def on_error(err):
    print(f"Stream error: {err}", flush=True)
    return False


ipc.subscribe_to_topic(topic=DATA_TOPIC, on_stream_event=on_data, on_stream_error=on_error)
print(f"Subscribed to {DATA_TOPIC}", flush=True)

# keep the component alive
while True:
    time.sleep(10)
