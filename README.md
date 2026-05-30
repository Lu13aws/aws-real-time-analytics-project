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