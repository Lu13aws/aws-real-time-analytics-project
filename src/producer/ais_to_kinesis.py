import json
import os

import boto3
from dotenv import load_dotenv
from websocket import WebSocketApp


load_dotenv()

AIS_API_KEY = os.getenv("AIS_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "ais-stream-v1")

if not AIS_API_KEY:
    raise ValueError("AIS_API_KEY not found in .env file")


kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)


SUBSCRIPTION_MESSAGE = {
    "APIKey": AIS_API_KEY,
    "BoundingBoxes": [
        [
            [50.0, 22.0],
            [60.0, 30.0]
        ]
    ],
    "FilterMessageTypes": ["PositionReport"]
}


def on_open(ws):
    print("Connected to AISStream")
    ws.send(json.dumps(SUBSCRIPTION_MESSAGE))
    print("Subscription message sent")


def on_message(ws, message):
    try:
        data = json.loads(message)

        message_type = data.get("MessageType", "Unknown")
        metadata = data.get("MetaData", {})
        position_report = data.get("Message", {}).get("PositionReport", {})

        mmsi = str(metadata.get("MMSI", "unknown"))

        kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=mmsi
        )

        print(
            f"Sent to Kinesis | "
            f"Type: {message_type} | "
            f"MMSI: {mmsi} | "
            f"Lat: {position_report.get('Latitude')} | "
            f"Lon: {position_report.get('Longitude')}"
        )

    except Exception as error:
        print(f"Error processing message: {error}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    print(f"Status code: {close_status_code}")
    print(f"Message: {close_msg}")


def main():
    websocket = WebSocketApp(
        "wss://stream.aisstream.io/v0/stream",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    websocket.run_forever()


if __name__ == "__main__":
    main()