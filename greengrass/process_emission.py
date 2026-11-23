import json
import threading
import time
import traceback

from awsiot.greengrasscoreipc.clientv2 import GreengrassCoreIPCClientV2
from awsiot.greengrasscoreipc.model import BinaryMessage, PublishMessage

DATA_TOPIC = "vehicle/+/data"
max_co2 = {}  # vehicle -> highest CO2 seen so far
lock = threading.Lock()  # callbacks run on multiple threads

ipc = GreengrassCoreIPCClientV2()


def publish_result(thing, result):
    # each car only listens on its own result topic
    ipc.publish_to_topic(
        topic=f"vehicle/{thing}/result",
        publish_message=PublishMessage(
            binary_message=BinaryMessage(message=json.dumps(result).encode())
        ),
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

        # only send back when the max goes up
        with lock:
            if co2 <= max_co2.get(thing, float("-inf")):
                return
            max_co2[thing] = co2
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