# Real-Time Maritime Analytics Pipeline on AWS

## Project Overview

This project is a real-time maritime analytics platform built on AWS using live AIS (Automatic Identification System) vessel data.

The system ingests streaming ship position data, processes it in real time, stores raw events for archival purposes, and visualizes vessel activity through OpenSearch Dashboards.

The project initially started with an Apache Flink-based architecture but was later redesigned into a serverless event-driven architecture due to connector compatibility issues between Apache Flink and Amazon OpenSearch Service.

---

# Final Architecture

## Technologies Used

* Python
* AWS Lambda
* Amazon Kinesis Data Streams
* Amazon SQS
* Amazon Kinesis Firehose
* Amazon OpenSearch Service
* OpenSearch Dashboards
* Amazon S3
* IAM

https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764674099465189&cot=14
---

# Final Data Flow

```text
AIS API
→ Local Python Producer
→ Amazon Kinesis Data Streams
→ Lambda Consumer Function
→ Amazon S3 Raw Archive
→ Amazon SQS
→ Lambda SQS-to-Firehose Function
→ Amazon Kinesis Firehose
→ Amazon OpenSearch Service
→ OpenSearch Dashboards
```

---

# Initial Apache Flink Architecture

The project originally used the following architecture:

https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764673850213933&cot=14

```text
AIS API
→ Python Producer
→ Kinesis Data Streams
→ Apache Flink SQL (Apache Zeppelin)
→ OpenSearch Sink
→ OpenSearch Dashboards
```

The goal was to process and transform the incoming AIS stream using Apache Flink SQL before writing the transformed stream into OpenSearch.

---

# Apache Flink / OpenSearch Connector Problem

During implementation, a major compatibility issue occurred between Apache Flink and Amazon OpenSearch Service.

The OpenSearch sink connector relied on Elasticsearch 7 libraries and JAR files, which turned out to be incompatible with the OpenSearch version used in this project.

This resulted in:

* unstable sink behavior
* failed writes to OpenSearch
* connector interruptions
* compatibility problems inside the Zeppelin interpreter environment

After extensive troubleshooting, it became clear that the issue was caused by connector and JAR incompatibilities rather than by the SQL logic itself.

Because of this limitation, the architecture was redesigned into a fully serverless AWS-native event-driven pipeline using Lambda, SQS, Firehose, and OpenSearch.

---

# OpenSearch Setup

## Creating the OpenSearch Domain

An OpenSearch domain was created in AWS.

After the domain status became active, the OpenSearch Dashboard endpoint became available.

Initially, access to OpenSearch Dashboards was blocked because the domain access policy denied access.

The issue was solved by modifying the access policy from:

```json
"Deny"
```

to:

```json
"Allow"
```

This enabled dashboard access.

---

# OpenSearch Index Creation

Inside OpenSearch Dashboards:

```text
Index Management
→ Indexes
→ Create Index
```

A custom index mapping JSON was created in order to define:

* field types
* geo_point fields
* timestamps
* numeric fields
* aggregatable keyword fields

The mapping defined how incoming AIS data would be stored and indexed.

---

# Apache Zeppelin Setup

An Apache Zeppelin notebook was created for Flink SQL processing.

Inside Zeppelin:

* a Studio notebook was created
* Apache Zeppelin paragraphs were added
* Flink SQL commands were executed
* test sinks and parsing tables were created

The raw AIS stream first needed to be parsed and transformed before writing to the final vessel sink.

---

# Migration to Serverless Architecture

After the Flink connector issues, the architecture was redesigned.

The final architecture introduced:

* Amazon SQS
* two AWS Lambda functions
* Amazon Kinesis Firehose
* OpenSearch integration

---

# SQS Integration

An Amazon SQS queue was created.

The Lambda environment variables were updated to include:

* SQS queue URL
* queue configuration values

The Lambda Python code was modified to publish messages into SQS.

After code changes, the Lambda function was redeployed.

---

# Firehose Integration

Amazon Kinesis Firehose was configured with:

## Source

```text
Direct PUT
```

## Destination

```text
Amazon OpenSearch Service
```

The following values had to be configured:

* existing OpenSearch domain
* target OpenSearch index
* S3 backup bucket

---

# S3 Backup Configuration

During Firehose setup, S3 prefixes were configured in order to keep backup files organized.

Without prefixes, all files would have been stored in the root bucket structure.

The existing raw archive bucket structure was reused.

---

# IAM Permissions

Additional IAM permissions were required for:

* Lambda
* SQS
* Firehose
* OpenSearch access

A second Lambda function was created because SQS cannot write directly into Firehose.

The second Lambda function:

* consumed SQS messages
* transformed records
* sent records into Firehose

---

# OpenSearch Security Role Mapping

Initially, Firehose was unable to write into OpenSearch.

The issue was caused by missing OpenSearch write permissions.

The fix was applied inside OpenSearch Security configuration:

```text
Security
→ Roles
→ all_access
→ Map Users
```

The Firehose service role ARN was added to backend roles.

After this change:

* Firehose successfully indexed documents
* OpenSearch document counts started increasing in real time

---

# Geo Mapping Issue

The first Geo Map visualization did not work because a combined geo_point field did not exist.

To solve this:

* the second Lambda function was modified
* a combined geo field was generated
* the Lambda function was redeployed

A new OpenSearch index and a new index pattern had to be created afterward.

Important:
Whenever the document structure changes inside the Lambda transformation logic, a new index should be created.

Additionally:
The Firehose destination index also needs to be updated whenever a new OpenSearch index is created.

Example:

```text
Old index:
ais-index-vessel-data-v1

New index:
ais-index-vessel-data-v2
```

The Firehose configuration must point to the new index.

---

# Dynamic AIS Regions

The AIS producer script uses configurable bounding boxes.

This allows switching between maritime regions by simply changing coordinates inside the producer script.

Examples:

* Baltic Sea
* Mediterranean Sea
* Persian Gulf
* Strait of Hormuz

No architecture changes are required when changing regions.

---

# OpenSearch Visualizations

Several real-time visualizations were created in OpenSearch Dashboards:

## Geo Map

Real-time ship locations displayed on a map.

## Total Active Vessels KPI

Unique vessel count using:

```text
Unique Count(ship_name)
```

## Total AIS Messages KPI

Real-time message ingestion count.

## Top Vessel Speed

Bar chart showing vessels with highest speed.

## Messages Per Minute

Line chart visualizing ingestion activity over time.

## Traffic Monitoring

Real-time monitoring of vessel density and maritime traffic spikes.

---

# MMSI vs Ship Name

Two identifiers were used:

## MMSI

Maritime Mobile Service Identity

A unique numeric identifier assigned to a vessel transponder.

## Ship Name

Human-readable vessel name.

Difference:

* MMSI is technically unique
* ship names are not guaranteed to be unique globally

Because of this:

* MMSI is more reliable for technical tracking
* ship_name is better for dashboard readability

---

# Lessons Learned

This project provided hands-on experience with:

* real-time streaming architectures
* event-driven AWS systems
* OpenSearch indexing
* geospatial analytics
* IAM permissions
* streaming transformations
* troubleshooting connector compatibility issues
* serverless architecture redesign

One of the biggest lessons learned was that architectural flexibility and troubleshooting are essential when building real-world data engineering systems.

---

# Future Improvements

Potential future improvements include:

* anomaly detection
* vessel type classification
* alerting systems
* historical analytics
* Athena integration
* Grafana dashboards
* Dockerized deployment
* CI/CD pipelines
* machine learning for maritime traffic prediction
