# AWS Real-Time Analytics Project

## Project Overview

This project is a real-time streaming analytics pipeline built on AWS.

The system ingests live AIS vessel tracking data from the AISStream API, processes streaming events in real time, stores raw and transformed data in Amazon S3, and enables future analytics with services such as AWS Kinesis, Lambda, Athena, Glue, and QuickSight.

The goal of this project is to simulate a production-grade cloud-native real-time data engineering workflow focused on maritime traffic monitoring and analytics.

---

## Architecture

https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764673850213933&cot=14

Planned AWS services used in this project:

- Amazon Kinesis Data Streams
- Managed Apache Flink
- AWS OpenSearch
- AWS Lambda
- Amazon S3
- AWS Glue
- Amazon Athena
- Amazon QuickSight
- Amazon CloudWatch
- AWS IAM

---

## Use Case

This project focuses on ingesting and analyzing real-time AIS vessel tracking data.

Possible analytics scenarios include:

- Monitoring vessel traffic in strategic maritime regions
- Tracking oil tankers and cargo ships
- Real-time event streaming and processing
- Maritime traffic analytics
- Latency and throughput monitoring

---

## Project Structure

```bash
aws-real-time-analytics-project/
│
├── docs/
├── infrastructure/
├── monitoring/
├── notebooks/
├── scripts/
├── src/
│   ├── consumer/
│   ├── producer/
│   └── transformation/
├── tests/
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```
## Streaming Optimization

The initial ingestion pipeline stored every incoming AIS event as an individual JSON object in Amazon S3.

During testing, this approach quickly generated thousands of very small files within a short period of time. This pattern can lead to several performance and scalability issues in cloud-based data lake architectures, including:

* Increased S3 PUT request overhead
* Poor Athena query performance
* Higher metadata and crawler overhead
* Small-files problem in distributed analytics systems

To improve scalability and storage efficiency, the ingestion logic was redesigned to use buffered event batching.

The updated ingestion pipeline now:

* Buffers incoming AIS events in memory
* Groups events into batches of 100 records
* Stores batched data as JSONL files in Amazon S3
* Uses time-based partitioning (`year/month/day/hour`)

This design significantly reduces the number of S3 objects while improving downstream analytics performance and reducing operational overhead.

The ingestion pipeline now behaves more closely to a production-grade real-time streaming architecture.

# Project Update – Real-Time AIS Streaming Pipeline with AWS

## Progress Overview

Today, the real-time AIS streaming pipeline was significantly extended and connected across multiple AWS services.

The project now supports:

* Real-time AIS vessel data ingestion
* Streaming through Amazon Kinesis Data Streams
* Raw data archiving in Amazon S3
* Real-time stream processing with Managed Apache Flink
* Interactive SQL analysis using Apache Zeppelin
* Initial architecture documentation

---

# Implemented Components

## 1. Amazon Kinesis Data Streams

Created and configured a Kinesis Data Stream to receive real-time AIS vessel events from the local Python client.

The AIS data is continuously streamed into AWS in JSON format.

---

## 2. AWS Lambda Consumer

Created a Lambda consumer function connected to the Kinesis Data Stream.

### Implemented:

* Lambda trigger configuration
* IAM permissions and policies
* Access from Lambda to:

  * Kinesis Data Streams
  * Amazon S3

The Lambda function stores incoming raw AIS events inside an S3 raw bucket for long-term archival purposes.

---

## 3. Amazon S3 Raw Storage

Created an S3 bucket for raw event storage.

Purpose:

* Long-term archival
* Future batch analytics
* Backup and replay capabilities

This establishes a cold-storage layer for the streaming architecture.

---

## 4. Python AIS Streaming Client

Extended the local Python client responsible for consuming AIS data from the external AIS source.

### Updates:

* Added Kinesis integration
* Updated `.env` configuration
* Added:

  * `KINESIS_STREAM_NAME`
* Configured AWS credentials and environment variables

The client now pushes live AIS events directly into Kinesis.

---

## 5. Managed Apache Flink

Created:

* Managed Apache Flink application
* Zeppelin notebook environment

Apache Zeppelin was used for interactive stream processing and SQL experimentation.

---

# Apache Zeppelin & Flink SQL

## Initial Flink Interpreter Test

Executed the following command to validate the Flink interpreter:

```sql
%flink.ssql(type=update)
SHOW TABLES;
```

Important:
The Python AIS streaming script was intentionally NOT running during the initial setup and validation phase.

---

## Kinesis Stream Registration

The Kinesis Data Stream was registered and connected to Flink using SQL DDL statements.

After the successful table registration:

* the Python AIS script was started
* real-time vessel data immediately appeared inside Zeppelin

This confirmed the successful end-to-end streaming integration.

---

# Real-Time Stream Analytics

Using Flink SQL, live AIS vessel data could be queried and visualized directly inside Zeppelin.

The most important query included:

* Latitude
* Longitude
* Vessel speed
* Vessel names

This enabled:

* table visualizations
* scatter plots
* real-time vessel monitoring

```
%flink.ssql(type=update)

SELECT
  CAST(JSON_VALUE(raw_data, '$.Message.PositionReport.Longitude') AS DOUBLE) AS lon,

  CAST(JSON_VALUE(raw_data, '$.Message.PositionReport.Latitude') AS DOUBLE) AS lat,

  JSON_VALUE(raw_data, '$.MetaData.ShipName') AS ship_name,

  CAST(JSON_VALUE(raw_data, '$.Message.PositionReport.Sog') AS DOUBLE) AS speed_knots

FROM ais_stream;
```

---

# Architecture Progress

An AWS architecture diagram was created to visualize the pipeline components and data flow.

Current architecture includes:
https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764673850213933&cot=14

* AIS Source
* Local Python Client
* Amazon Kinesis Data Streams
* AWS Lambda
* Amazon S3
* Managed Apache Flink
* Apache Zeppelin
* Amazon OpenSearch (planned)
* IAM

---

# Current Pipeline

```text
AIS Source
   ↓
Local Python Client
   ↓
Amazon Kinesis Data Streams
   ├── AWS Lambda → S3 Raw Bucket
   └── Managed Apache Flink → Zeppelin Analytics
```

---
