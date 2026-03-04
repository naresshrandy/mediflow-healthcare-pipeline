import json
import time
import random
import os
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

DEPARTMENTS = [
    "Emergency", "Cardiology", "Neurology",
    "Orthopedics", "Pediatrics", "Oncology",
    "ICU", "General Medicine"
]

DIAGNOSES = [
    "Hypertension", "Type 2 Diabetes", "Pneumonia",
    "Heart Failure", "Stroke", "Fracture",
    "Appendicitis", "COVID-19", "Sepsis",
    "Kidney Disease", "Chest Pain", "Asthma"
]

BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
SEVERITY_LEVELS = ["Low", "Medium", "High", "Critical"]
DOCTORS = [
    "Dr. Sarah Johnson", "Dr. Michael Chen",
    "Dr. Emily Rodriguez", "Dr. James Wilson",
    "Dr. Priya Patel", "Dr. David Kim"
]

# ------------------------------------
# Dead Letter Queue
# Stores failed records locally when Kafka is down
# ------------------------------------
DEAD_LETTER_FILE = "data/dead_letter_queue.json"

def save_to_dead_letter(record, error):
    os.makedirs("data", exist_ok=True)
    entry = {
        "record": record,
        "error": str(error),
        "failed_at": datetime.now().isoformat(),
        "retry_count": 0
    }
    with open(DEAD_LETTER_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"💾 Saved to dead letter queue: {record['patient_id']}")

def load_dead_letter_queue():
    if not os.path.exists(DEAD_LETTER_FILE):
        return []
    records = []
    with open(DEAD_LETTER_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def clear_dead_letter_queue():
    if os.path.exists(DEAD_LETTER_FILE):
        os.remove(DEAD_LETTER_FILE)
        print("✅ Dead letter queue cleared")

# ------------------------------------
# Generate patient record
# ------------------------------------
def generate_patient():
    admission_time = datetime.now() - timedelta(minutes=random.randint(0, 60))
    return {
        "patient_id": f"PAT-{fake.unique.random_number(digits=6)}",
        "name": fake.name(),
        "age": random.randint(1, 95),
        "gender": random.choice(["Male", "Female"]),
        "blood_type": random.choice(BLOOD_TYPES),
        "department": random.choice(DEPARTMENTS),
        "diagnosis": random.choice(DIAGNOSES),
        "severity": random.choice(SEVERITY_LEVELS),
        "doctor": random.choice(DOCTORS),
        "admission_time": admission_time.isoformat(),
        "insurance_id": f"INS-{fake.unique.random_number(digits=6)}",
        "referring_doctor": f"{fake.name()}, {fake.city()} Hospital",
        "ward_number": f"Ward-{random.choice(['1A','1B','2A','2B','3A','3B','4A','4B'])}",
        "vitals": {
            "heart_rate": random.randint(55, 130),
            "blood_pressure_systolic": random.randint(90, 180),
            "blood_pressure_diastolic": random.randint(60, 110),
            "temperature": round(random.uniform(96.0, 104.0), 1),
            "oxygen_saturation": random.randint(88, 100),
            "respiratory_rate": random.randint(12, 30)
        },
        "lab_results": {
            "glucose": random.randint(70, 400),
            "hemoglobin": round(random.uniform(8.0, 17.0), 1),
            "white_blood_cells": round(random.uniform(3.5, 15.0), 1),
            "creatinine": round(random.uniform(0.5, 5.0), 2),
            "sodium": random.randint(130, 150),
            "potassium": round(random.uniform(3.0, 6.0), 1)
        },
        "insurance": random.choice(["Medicare", "Medicaid", "BlueCross", "Aetna", "United", "Self-Pay"]),
        "emergency_contact": fake.phone_number(),
        "created_at": datetime.now().isoformat()
    }

def generate_lab_result(patient_id):
    return {
        "lab_id": f"LAB-{fake.unique.random_number(digits=7)}",
        "patient_id": patient_id,
        "test_name": random.choice([
            "Complete Blood Count", "Metabolic Panel",
            "Lipid Panel", "Thyroid Function",
            "Liver Function", "Urinalysis",
            "Blood Culture", "COVID Test"
        ]),
        "result_value": round(random.uniform(0.1, 500.0), 2),
        "unit": random.choice(["mg/dL", "mmol/L", "g/dL", "IU/L", "%", "cells/mcL"]),
        "reference_range": "Normal: 70-100",
        "status": random.choice(["Normal", "Abnormal", "Critical"]),
        "lab_technician": fake.name(),
        "collected_at": datetime.now().isoformat(),
        "resulted_at": (datetime.now() + timedelta(hours=random.randint(1, 6))).isoformat()
    }

# ------------------------------------
# Create Kafka producer with retries
# ------------------------------------
def create_producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None,
        retries=3,
        retry_backoff_ms=1000,
        request_timeout_ms=5000
    )

# ------------------------------------
# Retry dead letter queue records
# ------------------------------------
def retry_dead_letter_queue(producer):
    records = load_dead_letter_queue()
    if not records:
        return

    print(f"\n🔄 Retrying {len(records)} records from dead letter queue...")
    success_count = 0

    for entry in records:
        try:
            record = entry['record']
            producer.send(
                topic='patient-admissions',
                key=record['patient_id'],
                value=record
            )
            producer.flush()
            success_count += 1
            print(f"✅ Recovered: {record['patient_id']} | "
                  f"Originally failed at: {entry['failed_at']}")
        except Exception as e:
            print(f"❌ Still failing: {record['patient_id']} — {e}")

    if success_count == len(records):
        clear_dead_letter_queue()
        print(f"✅ All {success_count} records recovered successfully!")
    else:
        print(f"⚠️ Recovered {success_count}/{len(records)} records")

# ------------------------------------
# Main streaming function
# ------------------------------------
def stream_patients(interval_seconds=5):
    print("🏥 MediFlow Resilient Patient Generator Starting...")
    print("💡 Dead letter queue enabled — no data will be lost!\n")

    producer = None
    kafka_available = False
    failed_count = 0
    success_count = 0

    while True:
        try:
            # Try to connect to Kafka if not connected
            if not kafka_available:
                print("📡 Attempting to connect to Kafka...")
                producer = create_producer()
                kafka_available = True
                print("✅ Connected to Kafka!")

                # Retry any failed records
                retry_dead_letter_queue(producer)

            # Generate patient
            patient = generate_patient()
            lab = generate_lab_result(patient['patient_id'])

            # Send to Kafka
            producer.send(
                topic='patient-admissions',
                key=patient['patient_id'],
                value=patient
            )
            producer.send(
                topic='lab-results',
                key=lab['patient_id'],
                value=lab
            )
            producer.flush()
            success_count += 1

            print(f"✅ [{success_count}] Patient: {patient['name']} | "
                  f"Dept: {patient['department']} | "
                  f"Severity: {patient['severity']}")

        except Exception as e:
            kafka_available = False
            failed_count += 1

            # Save to dead letter queue instead of losing data
            try:
                patient
                save_to_dead_letter(patient, e)
                print(f"⚠️  Kafka down! Saved to dead letter queue. "
                      f"Failed: {failed_count} | "
                      f"Will retry when Kafka recovers...")
            except NameError:
                print(f"❌ Kafka connection failed: {e}")

            if producer:
                try:
                    producer.close()
                except:
                    pass
                producer = None

            # Wait before retrying connection
            time.sleep(10)
            continue

        time.sleep(interval_seconds)

if __name__ == "__main__":
    stream_patients(interval_seconds=5)