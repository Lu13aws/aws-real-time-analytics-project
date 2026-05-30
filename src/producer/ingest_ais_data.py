import json
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from websocket import WebSocketApp


# Load environment variables from .env
load_dotenv()

AIS_API_KEY = os.getenv("AIS_API_KEY")
S3_RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

if not AIS_API_KEY:
    raise ValueError("AIS_API_KEY not found in .env file")

if not S3_RAW_BUCKET:
    raise ValueError("S3_RAW_BUCKET not found in .env file")


# Create S3 client
s3_client = boto3.client("s3", region_name=AWS_REGION)


# Bounding box for Strait of Hormuz
SUBSCRIPTION_MESSAGE = {
    "APIKey": AIS_API_KEY,
    "BoundingBoxes": [
        [
            [25.0, 55.0],
            [28.5, 58.5]
        ]
    ],
    "FilterMessageTypes": ["PositionReport"]
}


def build_s3_key(timestamp: datetime) -> str:
    return (
        f"raw/ais/strait_of_hormuz/"
        f"year={timestamp.year}/"
        f"month={timestamp.month:02}/"
        f"day={timestamp.day:02}/"
        f"hour={timestamp.hour:02}/"
        f"ais_message_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"
    )


def upload_to_s3(data: dict) -> None:
    timestamp = datetime.now(timezone.utc)
    s3_key = build_s3_key(timestamp)

    s3_client.put_object(
        Bucket=S3_RAW_BUCKET,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Uploaded to S3: s3://{S3_RAW_BUCKET}/{s3_key}")


def on_open(ws):
    print("Connected to AISStream")
    ws.send(json.dumps(SUBSCRIPTION_MESSAGE))
    print("Subscription message sent")


def on_message(ws, message):
    try:
        data = json.loads(message)

        metadata = data.get("MetaData", {})
        position_report = data.get("Message", {}).get("PositionReport", {})

        ship_name = metadata.get("ShipName", "Unknown")
        mmsi = metadata.get("MMSI", "Unknown")
        latitude = position_report.get("Latitude")
        longitude = position_report.get("Longitude")
        speed = position_report.get("Sog")
        course = position_report.get("Cog")

        print("=" * 60)
        print(f"Ship: {ship_name}")
        print(f"MMSI: {mmsi}")
        print(f"Latitude: {latitude}")
        print(f"Longitude: {longitude}")
        print(f"Speed over ground: {speed}")
        print(f"Course over ground: {course}")

        upload_to_s3(data)

    except Exception as error:
        print(f"Error processing AIS message: {error}")


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