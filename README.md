# Real-Time Maritime Analytics Pipeline on AWS

## Project Description

Real-time maritime analytics platform built on AWS using live AIS (Automatic Identification System) vessel data. The system ingests streaming ship position data through a serverless event-driven pipeline, stores raw events in S3 for archival, and visualizes vessel activity in real time through OpenSearch Dashboards.

The project started with an Apache Flink-based architecture and was later redesigned into a fully serverless pipeline after hitting a critical connector incompatibility between Flink and Amazon OpenSearch Service.

---

## Project Structure

```
aws-real-time-analytics-project/
├── src/
│   ├── producer/           # AIS WebSocket producer scripts
│   │   ├── ais_to_kinesis.py       # Stateless real-time Kinesis producer (v1)
│   │   └── ingest_ais_data.py      # Buffered S3 archival producer (v2)
│   ├── consumer/           # Lambda consumer code (deployed in AWS console)
│   └── transformation/     # Transformation logic (deployed in AWS console)
├── skills/                 # Reusable skill documentation extracted from this project
│   ├── opensearch_setup_config/
│   ├── aws_streaming_stack/
│   ├── opensearch_dashboards/
│   ├── timeseries_analytics/
│   ├── data_ingestion/
│   └── documentation_patterns/
├── data/
│   └── screenshots/        # Dashboard and configuration screenshots
├── infrastructure/
│   ├── cloudformation/     # Placeholder — not yet implemented
│   └── terraform/          # Placeholder — not yet implemented
├── notebooks/              # Placeholder — Jupyter notebooks for historical analysis
├── .env                    # Local secrets (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Final Architecture

### Technologies Used

| Layer | Technology |
|---|---|
| Language | Python 3.x |
| Streaming bus | Amazon Kinesis Data Streams |
| Compute | AWS Lambda (×2) |
| Queueing | Amazon SQS |
| Managed delivery | Amazon Kinesis Firehose |
| Storage | Amazon S3 |
| Search & analytics | Amazon OpenSearch Service |
| Visualization | OpenSearch Dashboards |
| Access control | AWS IAM |

Architecture diagram: https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764674099465189&cot=14

---

## Final Data Flow

```
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

## Initial Architecture

```
AIS API
→ Python Producer
→ Kinesis Data Streams
→ Apache Flink SQL (Apache Zeppelin)
→ OpenSearch Sink
→ OpenSearch Dashboards
```

**Goal:** Process and transform the incoming AIS stream using Apache Flink SQL before writing the results into OpenSearch.

Architecture diagram: https://miro.com/app/board/uXjVHS6eY4g=/?moveToWidget=3458764673850213933&cot=14

**Why it failed:**
The OpenSearch sink connector depended on Elasticsearch 7 JARs, which were incompatible with the AWS OpenSearch version used in this project. This caused unstable sink behavior, failed writes, and connector interruptions inside the Zeppelin interpreter. After extensive troubleshooting it became clear the issue was at the connector level, not the SQL logic.

**Decision:** Abandon Flink and redesign as a fully serverless AWS-native pipeline using Lambda, SQS, Firehose, and OpenSearch.

---

## Service Setup

### OpenSearch Domain

An OpenSearch domain was created in AWS. After the domain status became Active, the Dashboard endpoint became available.

The default access policy was set to `"Deny"`, blocking all Dashboard access. Fixed by changing the policy to `"Allow"`:

```
AWS Console → OpenSearch Service → domain → Actions → Edit security configuration
```

### OpenSearch Index

A custom index mapping JSON was created inside Dashboards to define field types before data arrived:

```
Index Management → Indexes → Create Index
```

Fields mapped: `geo_point`, `date`, `keyword` (aggregatable identifiers), `float` (metrics).

### SQS Queue

An SQS queue was created. The first Lambda function's environment variables were updated with the queue URL and the function was redeployed to publish messages into SQS.

### Kinesis Firehose

Firehose was configured with:

| Setting | Value |
|---|---|
| Source | Direct PUT |
| Destination | Amazon OpenSearch Service |
| Index | `ais-index-vessel-data-v2` |
| S3 backup | Existing raw archive bucket with prefix |

### OpenSearch Security Role Mapping

Firehose could not write to OpenSearch until the Firehose service role ARN was added to OpenSearch backend roles:

```
OpenSearch Dashboards → Security → Roles → all_access → Map Users → Backend roles
```

### Apache Zeppelin (abandoned)

A Zeppelin Studio notebook was created for Flink SQL processing. Test sinks and parsing tables were created. This setup was abandoned after the Flink/OpenSearch connector issue was identified.

---

## Environment Setup

### Required environment variables (`.env`)

```
AIS_API_KEY=            # AISStream.io API key
S3_RAW_BUCKET=          # S3 bucket name for raw JSONL archive
AWS_REGION=             # AWS region (default: eu-central-1)
KINESIS_STREAM_NAME=    # Kinesis stream name (default: ais-stream-v1)
```

### Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run the producers

```bash
# Buffered S3 archival producer (recommended for raw data storage)
python src/producer/ingest_ais_data.py

# Stateless real-time Kinesis streaming producer
python src/producer/ais_to_kinesis.py
```

Stop with `Ctrl+C` — the buffered producer flushes remaining events to S3 on exit.

---

## Data Source

| Property | Value |
|---|---|
| Source | AISStream.io |
| Protocol | WebSocket (`wss://stream.aisstream.io/v0/stream`) |
| Coverage | Global maritime AIS — project focused on Persian Gulf, Strait of Hormuz, Baltic Sea |
| Update frequency | Real-time continuous stream |
| Message filter | `PositionReport` messages only |

### Key fields

| Field | Description |
|---|---|
| `MMSI` | Maritime Mobile Service Identity — unique numeric vessel transponder ID |
| `ShipName` | Human-readable vessel name (not globally unique) |
| `Latitude` / `Longitude` | Decimal degree position |
| `Sog` | Speed Over Ground (knots) |
| `Cog` | Course Over Ground (degrees true) |

### Geographic region switching

Bounding boxes are the only thing that changes when switching regions — no infrastructure changes required:

| Region | BoundingBox |
|---|---|
| Persian Gulf | `[[29, 28], [37, 36]]` |
| Strait of Hormuz | `[[50, 22], [60, 30]]` |
| Baltic Sea | `[[50, 22], [60, 30]]` |
| Mediterranean | `[[30, -6], [46, 36]]` |

---

## AWS Budget

Monthly budget cap: *(set a cap in AWS Billing → Budgets to prevent surprises)*

Primary cost drivers: Kinesis shard hours, Lambda invocations, OpenSearch instance hours, S3 storage.

---

## Challenges & Fixes

### Apache Flink / OpenSearch Connector Incompatibility

**Symptom:** Flink SQL jobs ran without errors but no documents appeared in OpenSearch. Connector behavior was unstable and intermittent inside the Zeppelin interpreter.

**Root cause:** The Flink OpenSearch sink connector used Elasticsearch 7 JARs, which were incompatible with the AWS OpenSearch version deployed in this project.

**Fix:** Abandoned Flink entirely. Redesigned the pipeline as Lambda → SQS → Firehose → OpenSearch. This removed the incompatible connector and replaced it with AWS-managed delivery.

---

### OpenSearch Dashboards Access Blocked

**Symptom:** Navigating to the OpenSearch Dashboard URL returned an access denied error immediately after domain creation.

**Root cause:** The default domain access policy is set to `"Deny"` for all principals.

**Fix:**
```
AWS Console → OpenSearch Service → domain → Actions → Edit security configuration
→ Change access policy from "Deny" to "Allow"
```

---

### Geo Map Visualization Blank

**Symptom:** The Geo Map panel in OpenSearch Dashboards displayed no data points despite records being indexed successfully.

**Root cause:** OpenSearch Geo Map requires a `geo_point` typed field. Latitude and longitude were stored as separate `float` fields, which are not recognized for geo aggregations.

**Fix:** Modified Lambda Function 2 to compute and emit a combined geo field before sending to Firehose:

```python
record["geo"] = {"lat": record["latitude"], "lon": record["longitude"]}
```

Created a new index with `geo_point` mapping (`ais-index-vessel-data-v2`) and a new Dashboards index pattern. Updated the Firehose destination to point to the new index.

---

### Firehose Could Not Write to OpenSearch

**Symptom:** Firehose delivery stream showed success status, but OpenSearch document counts did not increase.

**Root cause:** The Firehose service role had no write permission to the OpenSearch domain. The domain accepted the connection but silently rejected the write operation.

**Fix:**
```
OpenSearch Dashboards → Security → Roles → all_access → Map Users → Backend roles
→ Add the Firehose service role ARN
```

---

### SQS Cannot Write to Firehose Directly

**Symptom:** No native SQS → Firehose integration available in AWS.

**Root cause:** AWS does not support SQS as a direct Firehose source. SQS and Firehose cannot be wired together without compute in between.

**Fix:** Created Lambda Function 2 as a bridge — triggered by SQS, it transforms records and calls `firehose.put_record()`. This also provided a natural place to add the `geo_point` enrichment step.

---

## Lessons Learned

- Flink's OpenSearch sink connector uses Elasticsearch 7 JARs — incompatible with AWS OpenSearch; avoid this path unless using a custom connector
- OpenSearch `geo_point` fields must be a combined `{"lat": x, "lon": y}` object — separate float fields do not work for Geo Map visualizations
- SQS cannot write to Firehose directly; a Lambda bridge is always required
- IAM role mapping inside OpenSearch (Security → backend roles) is separate from AWS IAM policies — this is easy to miss and causes silent write failures
- OpenSearch access policy defaults to Deny — change it immediately after domain creation
- Any document structure change requires a new index version plus updates to Firehose destination and Dashboards index pattern
- MMSI is technically unique per transponder; ship names are not globally unique — use MMSI for tracking logic, ship name for display
- Architectural flexibility is essential: the Flink pivot added time but resulted in a simpler, more maintainable system

---

## Future Improvements

### Analytics Enhancements
- Anomaly detection for vessels deviating from expected routes
- Vessel type classification (cargo, tanker, fishing, passenger)
- Zone-based alerting (vessel enters restricted or monitored area)
- Traffic density heatmaps by time of day

### Historical Analytics
- Athena integration for SQL queries over the S3 raw JSONL archive
- Glue Data Catalog for automatic partition discovery
- Historical route reconstruction per vessel

### Infrastructure
- CloudFormation or Terraform templates for all manually created resources
- Dockerized producer deployment for portability
- CI/CD pipeline for Lambda function deployments

### Visualization
- Grafana dashboards as an alternative to OpenSearch Dashboards
- Vessel speed and heading overlays on the Geo Map

### ML Extensions
- Route prediction using historical AIS traces
- Maritime traffic forecasting by region and time window

---

## Project Progress

### 20260531

- Set up AISStream.io account and obtained API key
- Created `ais_to_kinesis.py` — initial WebSocket producer writing to Kinesis
- Created Kinesis Data Stream `ais-stream-v1` in `eu-central-1`
- Created OpenSearch domain; resolved Dashboard access (Deny → Allow policy)
- Created Lambda Consumer Function with Kinesis trigger
- Initial Flink/Zeppelin setup attempted for stream processing

### 20260601

- Identified Flink/OpenSearch connector JAR incompatibility after extensive troubleshooting
- Decided to pivot to serverless Lambda/SQS/Firehose architecture
- Created SQS queue; updated Lambda Function 1 to publish to SQS
- Created Kinesis Firehose with Direct PUT → OpenSearch destination
- Created Lambda Function 2 (SQS → Firehose bridge)
- Resolved Firehose write permission by mapping service role ARN to OpenSearch `all_access` backend role

### 20260602

- Identified blank Geo Map issue — missing `geo_point` field
- Modified Lambda Function 2 to emit combined `geo` field
- Created `ais-index-vessel-data-v2` with `geo_point` mapping
- Updated Firehose destination to new index
- Geo Map rendering confirmed with live vessel data
- Built full OpenSearch Dashboard: Geo Map, KPI cards, speed bar chart, messages-per-minute line chart
- Changed bounding boxes from Persian Gulf to Strait of Hormuz region
- Created `ingest_ais_data.py` — buffered S3 archival producer with Hive-style time partitioning
