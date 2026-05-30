# AWS Real-Time Analytics Project

## Project Overview

This project is a real-time streaming analytics pipeline built on AWS.

The system ingests live AIS vessel tracking data from the AISStream API, processes streaming events in real time, stores raw and transformed data in Amazon S3, and enables future analytics with services such as AWS Kinesis, Lambda, Athena, Glue, and QuickSight.

The goal of this project is to simulate a production-grade cloud-native real-time data engineering workflow focused on maritime traffic monitoring and analytics.

---

## Architecture

Planned AWS services used in this project:

- Amazon Kinesis Data Streams
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

