import json
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from websocket import WebSocketApp


load_dotenv()

AIS_API_KEY = os.getenv("AIS_API_KEY")
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

if not AIS_API_KEY:
    raise ValueError("AIS_API_KEY not found in .env file")

if not S3_RAW_BUCKET:
    raise ValueError("S3_RAW_BUCKET not found in .env file")


BUFFER_SIZE = 100
event_buffer = []

s3_client = boto3.client("s3", region_name=AWS_REGION)


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


def build_s3_key(timestamp: datetime) -> str:
    return (
        "raw/ais/strait_of_hormuz/"
        f"year={timestamp.year}/"
        f"month={timestamp.month:02}/"
        f"day={timestamp.day:02}/"
        f"hour={timestamp.hour:02}/"
        f"batch_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
    )


def flush_buffer_to_s3() -> None:
    global event_buffer

    if not event_buffer:
        return

    timestamp = datetime.now(timezone.utc)
    s3_key = build_s3_key(timestamp)

    jsonl_body = "\n".join(json.dumps(event) for event in event_buffer)

    s3_client.put_object(
        Bucket=S3_RAW_BUCKET,
        Key=s3_key,
        Body=jsonl_body,
        ContentType="application/json"
    )

    print(f"Uploaded batch to S3: s3://{S3_RAW_BUCKET}/{s3_key}")
    print(f"Batch size: {len(event_buffer)} events")

    event_buffer = []


def on_open(ws):
    print("Connected to AISStream")
    print("Sending subscription message:")
    print(json.dumps(SUBSCRIPTION_MESSAGE, indent=2))
    ws.send(json.dumps(SUBSCRIPTION_MESSAGE))
    print("Subscription message sent")


def on_message(ws, message):
    global event_buffer

    try:
        data = json.loads(message)

        message_type = data.get("MessageType", "Unknown")
        metadata = data.get("MetaData", {})
        position_report = data.get("Message", {}).get("PositionReport", {})

        ship_name = metadata.get("ShipName", "Unknown")
        mmsi = metadata.get("MMSI", "Unknown")
        latitude = position_report.get("Latitude")
        longitude = position_report.get("Longitude")
        speed = position_report.get("Sog")
        course = position_report.get("Cog")

        event_buffer.append(data)

        print(
            f"Buffered event {len(event_buffer)}/{BUFFER_SIZE} | "
            f"Type: {message_type} | "
            f"Ship: {ship_name} | "
            f"MMSI: {mmsi} | "
            f"Lat: {latitude} | "
            f"Lon: {longitude} | "
            f"SOG: {speed} | "
            f"COG: {course}"
        )

        if len(event_buffer) >= BUFFER_SIZE:
            flush_buffer_to_s3()

    except Exception as error:
        print(f"Error processing AIS message: {error}")


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    print(f"Status code: {close_status_code}")
    print(f"Message: {close_msg}")
    flush_buffer_to_s3()


def main():
    websocket = WebSocketApp(
        "wss://stream.aisstream.io/v0/stream",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    try:
        websocket.run_forever()
    except KeyboardInterrupt:
        print("Script stopped manually")
        flush_buffer_to_s3()


if __name__ == "__main__":
    main()