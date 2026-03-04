import psycopg2
import hashlib
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-credentials.json"

PROJECT_ID = "mediflow-healthcare"
DATASET_ID = "patient_data"

def get_db():
    # connect to postgres
    return psycopg2.connect(
    host = "127.0.0.1",
    port = 5433,
    database = "mediflow_db",
    user = "mediflow",
    password = "mediflow123"
    )
    

def mask_name(name):
    # replace real name with anonymous ID
    # hint: use hashlib.sha256
    
    if name is None:
        print("⚠️ Found NULL value during masking")
        return None
    return hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:12]

    

def mask_age(age):
    # convert exact age to age range
    # hint: age // 10 gives you the decade
    if age is None:
        return "Unknown"
    return f"{(age // 10) * 10}-{(age // 10) * 10 + 9}"
    

def mask_record(record):
    # take one patient record (a dict)
    # return masked version
    # fields to mask: name, insurance_id, emergency_contact, age, doctor
    masked = record.copy()   # make a copy so original is safe
    masked['name'] = mask_name(record['name'])
    masked['age'] = mask_age(record['age'])
    masked['insurance_id'] = mask_name(record['insurance_id'])# hint: use mask_name() on insurance_id
    masked['emergency_contact'] = "REDACTED"
    masked['doctor'] = "REDACTED"
    return masked

def fetch_patients():
    # fetch all patients from postgres as a list of dicts
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM patients")
    rows = cursor.fetchall()
    # hint: you need column names to make dicts
    columns = [desc[0] for desc in cursor.description]
    patients = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return patients

def push_masked_to_bigquery(masked_df):
    # push to bigquery table called patients_masked
   
    client = bigquery.Client(project="mediflow-healthcare")
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.patients_masked"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    job = client.load_table_from_dataframe(masked_df, table_id, job_config=job_config)
    job.result()
    
    print(f"✅ Pushed {len(masked_df)} masked records to {table_id}")

def run():
    print("🔒 HIPAA Masking Pipeline Starting...")
    patients = fetch_patients()
    print(f"📥 Fetched {len(patients)} patients")
    
    # mask each patient
    masked = [mask_record(p) for p in patients]

    
    # convert to dataframe
    df = pd.DataFrame(masked)
    
    # push to bigquery
    push_masked_to_bigquery(df)
    
    print("✅ Masked data pushed to BigQuery!")
    print("🔒 Original data stays protected!")

if __name__ == "__main__":
    run()
