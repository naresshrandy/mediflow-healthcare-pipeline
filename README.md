# 🏥 MediFlow — Healthcare Data Pipeline

Enterprise-grade real-time patient data pipeline built with Apache Kafka, PostgreSQL, Apache Airflow, Google BigQuery, and Looker Studio.

## 🏗️ Architecture
`
Patient Generator → Kafka → Consumer → PostgreSQL → Airflow → BigQuery → Looker Studio
`

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Kafka | 7.4.0 | Real-time streaming |
| Apache Airflow | 2.6.3 | Pipeline orchestration |
| PostgreSQL | 15 | Operational database |
| Google BigQuery | Latest | Cloud data warehouse |
| Docker | 29.2.1 | Container orchestration |
| Python | 3.11 | Pipeline logic |

## 🚀 Quick Start

### 1. Start infrastructure
`ash
docker-compose up -d
`

### 2. Initialize Airflow
`ash
docker exec -it mediflow_airflow airflow db init
docker exec -d mediflow_airflow airflow webserver
docker exec -d mediflow_airflow airflow scheduler
`

### 3. Run the pipeline
`ash
# Activate virtual environment
venv\Scripts\activate

# Stream patient data
python ingestion\patient_generator.py

# Consume and save to PostgreSQL
python ingestion\kafka_consumer.py

# Push to BigQuery
python warehouse\bigquery_pipeline.py

# Apply HIPAA masking
python warehouse\hipaa_masking.py
`

## 📊 Features

- ✅ Real-time patient data streaming via Kafka
- ✅ Data quality validation at ingestion
- ✅ Dead Letter Queue for failure recovery
- ✅ Automated hourly Airflow pipeline
- ✅ BigQuery cloud warehousing
- ✅ HIPAA-compliant data masking
- ✅ Looker Studio live dashboard

## 🔴 Enterprise Challenges Solved

1. **Schema Evolution** — Added 3 new fields without downtime
2. **Pipeline Failure Recovery** — Dead Letter Queue with auto-replay
3. **Duplicate Detection** — Two-layer deduplication strategy
4. **HIPAA Compliance** — SHA-256 hashing and PII masking

## 📁 Project Structure
`
mediflow/
├── ingestion/
│   ├── patient_generator.py   # Resilient Kafka producer
│   ├── kafka_consumer.py      # Consumer with data validation
│   ├── duplicate_simulator.py # Duplicate testing tool
│   └── deduplicator.py        # Deduplication engine
├── orchestration/
│   └── airflow/
│       └── mediflow_pipeline.py  # Airflow DAG
├── warehouse/
│   ├── bigquery_pipeline.py   # PostgreSQL to BigQuery
│   └── hipaa_masking.py       # HIPAA masking pipeline
├── docker-compose.yml
└── requirements.txt
`

## ⚙️ Requirements

- Docker Desktop
- Python 3.11
- GCP account with BigQuery API enabled
- GCP service account credentials

## 📄 License

MIT License
