import os
import json
import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------
# Configuration
# ------------------------------------
PROJECT_ID = "mediflow-healthcare"
DATASET_ID = "patient_data"
CREDENTIALS_PATH = "gcp-credentials.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

# ------------------------------------
# Connect to PostgreSQL
# ------------------------------------
def get_postgres_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="mediflow_db",
        user="mediflow",
        password="mediflow123"
    )

# ------------------------------------
# Connect to BigQuery
# ------------------------------------
def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)

# ------------------------------------
# Define BigQuery table schemas
# ------------------------------------
PATIENTS_SCHEMA = [
    bigquery.SchemaField("patient_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("blood_type", "STRING"),
    bigquery.SchemaField("department", "STRING"),
    bigquery.SchemaField("diagnosis", "STRING"),
    bigquery.SchemaField("severity", "STRING"),
    bigquery.SchemaField("doctor", "STRING"),
    bigquery.SchemaField("admission_time", "TIMESTAMP"),
    bigquery.SchemaField("heart_rate", "INTEGER"),
    bigquery.SchemaField("blood_pressure_systolic", "INTEGER"),
    bigquery.SchemaField("blood_pressure_diastolic", "INTEGER"),
    bigquery.SchemaField("temperature", "FLOAT"),
    bigquery.SchemaField("oxygen_saturation", "INTEGER"),
    bigquery.SchemaField("respiratory_rate", "INTEGER"),
    bigquery.SchemaField("insurance", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("synced_at", "TIMESTAMP"),
    bigquery.SchemaField("insurance_id", "STRING"),
    bigquery.SchemaField("referring_doctor", "STRING"),
    bigquery.SchemaField("ward_number", "STRING"),
]

LAB_RESULTS_SCHEMA = [
    bigquery.SchemaField("lab_id", "STRING"),
    bigquery.SchemaField("patient_id", "STRING"),
    bigquery.SchemaField("test_name", "STRING"),
    bigquery.SchemaField("result_value", "FLOAT"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("lab_technician", "STRING"),
    bigquery.SchemaField("collected_at", "TIMESTAMP"),
    bigquery.SchemaField("resulted_at", "TIMESTAMP"),
    bigquery.SchemaField("synced_at", "TIMESTAMP"),
]

# ------------------------------------
# Create BigQuery table if not exists
# ------------------------------------
def create_table_if_not_exists(client, table_id, schema):
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} already exists")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"✅ Created table {table_id}")

# ------------------------------------
# Fetch patients from PostgreSQL
# ------------------------------------
def fetch_patients_from_postgres():
    print("📥 Fetching patients from PostgreSQL...")
    conn = get_postgres_connection()

    query = """
        SELECT
            patient_id, name, age, gender, blood_type,
            department, diagnosis, severity, doctor,
            admission_time, heart_rate, blood_pressure_systolic,
            blood_pressure_diastolic, temperature, oxygen_saturation,
            respiratory_rate, insurance, created_at,insurance_id, referring_doctor, ward_number
        FROM patients
        ORDER BY created_at DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"✅ Fetched {len(df)} patients from PostgreSQL")
    return df

# ------------------------------------
# Fetch lab results from PostgreSQL
# ------------------------------------
def fetch_labs_from_postgres():
    print("📥 Fetching lab results from PostgreSQL...")
    conn = get_postgres_connection()

    query = """
        SELECT
            lab_id, patient_id, test_name, result_value,
            unit, status, lab_technician, collected_at, resulted_at
        FROM lab_results
        ORDER BY collected_at DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"✅ Fetched {len(df)} lab results from PostgreSQL")
    return df

# ------------------------------------
# Push DataFrame to BigQuery
# ------------------------------------
def push_to_bigquery(client, df, table_id, schema):
    if df.empty:
        print(f"⚠️ No data to push to {table_id}")
        return 0

    # Add sync timestamp
    df['synced_at'] = datetime.now()

    # Convert timestamp columns
    timestamp_cols = ['admission_time', 'created_at', 'collected_at',
                     'resulted_at', 'synced_at']
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.tz_localize(None)

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Push to BigQuery
    print(f"🚀 Pushing {len(df)} rows to {table_id}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for job to complete

    print(f"✅ Successfully pushed {len(df)} rows to BigQuery!")
    return len(df)

# ------------------------------------
# Generate BigQuery Analytics
# ------------------------------------
def run_analytics(client):
    print("\n📊 Running analytics on BigQuery...")

    queries = {
        "Patients by Department": f"""
            SELECT department, COUNT(*) as patient_count
            FROM `{PROJECT_ID}.{DATASET_ID}.patients`
            GROUP BY department
            ORDER BY patient_count DESC
        """,
        "Critical Patients": f"""
            SELECT severity, COUNT(*) as count
            FROM `{PROJECT_ID}.{DATASET_ID}.patients`
            GROUP BY severity
            ORDER BY count DESC
        """,
        "Average Vitals by Department": f"""
            SELECT
                department,
                ROUND(AVG(heart_rate), 1) as avg_heart_rate,
                ROUND(AVG(oxygen_saturation), 1) as avg_oxygen,
                ROUND(AVG(temperature), 1) as avg_temp
            FROM `{PROJECT_ID}.{DATASET_ID}.patients`
            GROUP BY department
            ORDER BY avg_heart_rate DESC
        """
    }

    for title, query in queries.items():
        print(f"\n📈 {title}:")
        print("-" * 40)
        results = client.query(query).to_dataframe()
        print(results.to_string(index=False))

# ------------------------------------
# Main Pipeline
# ------------------------------------
def run_pipeline():
    print("🚀 MediFlow BigQuery Pipeline Starting...")
    print(f"📡 Project: {PROJECT_ID}")
    print(f"📦 Dataset: {DATASET_ID}\n")

    # Connect to BigQuery
    client = get_bigquery_client()
    print("✅ Connected to BigQuery!")

    # Table IDs
    patients_table = f"{PROJECT_ID}.{DATASET_ID}.patients"
    labs_table = f"{PROJECT_ID}.{DATASET_ID}.lab_results"

    # Create tables if they don't exist
    create_table_if_not_exists(client, patients_table, PATIENTS_SCHEMA)
    create_table_if_not_exists(client, labs_table, LAB_RESULTS_SCHEMA)

    # Fetch from PostgreSQL
    patients_df = fetch_patients_from_postgres()
    labs_df = fetch_labs_from_postgres()

    # Push to BigQuery
    patients_pushed = push_to_bigquery(
        client, patients_df, patients_table, PATIENTS_SCHEMA
    )
    labs_pushed = push_to_bigquery(
        client, labs_df, labs_table, LAB_RESULTS_SCHEMA
    )

    # Run analytics
    run_analytics(client)

    print(f"\n🎉 Pipeline Complete!")
    print(f"   Patients pushed to BigQuery: {patients_pushed}")
    print(f"   Lab results pushed to BigQuery: {labs_pushed}")

if __name__ == "__main__":
    run_pipeline()