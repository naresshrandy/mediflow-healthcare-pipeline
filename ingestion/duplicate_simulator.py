import json
import time
import random
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker()

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )

def simulate_duplicates():
    print("🐛 Duplicate Simulator Starting...")
    print("Simulating hospital system bug — sending duplicate records!\n")

    producer = create_producer()

    # Generate 10 patients
    patients = []
    for i in range(10):
        patient = {
            "patient_id": f"PAT-DUP-{i:04d}",
            "name": fake.name(),
            "age": random.randint(1, 95),
            "gender": random.choice(["Male", "Female"]),
            "blood_type": random.choice(["A+", "B+", "O+", "AB+"]),
            "department": random.choice(["Emergency", "ICU", "Cardiology"]),
            "diagnosis": random.choice(["Chest Pain", "Stroke", "Pneumonia"]),
            "severity": random.choice(["High", "Critical"]),
            "doctor": "Dr. Sarah Johnson",
            "admission_time": datetime.now().isoformat(),
            "insurance_id": f"INS-{i:06d}",
            "referring_doctor": "Dr. Smith, City Hospital",
            "ward_number": "Ward-1A",
            "vitals": {
                "heart_rate": random.randint(80, 120),
                "blood_pressure_systolic": random.randint(120, 160),
                "blood_pressure_diastolic": random.randint(80, 100),
                "temperature": round(random.uniform(98.0, 103.0), 1),
                "oxygen_saturation": random.randint(90, 99),
                "respiratory_rate": random.randint(16, 24)
            },
            "lab_results": {},
            "insurance": "Medicare",
            "emergency_contact": fake.phone_number(),
            "created_at": datetime.now().isoformat()
        }
        patients.append(patient)

    # Send each patient TWICE — simulating the bug
    print("Sending patients TWICE to simulate duplicate bug...\n")
    for i, patient in enumerate(patients):
        # First send
        producer.send('patient-admissions', key=patient['patient_id'], value=patient)
        print(f"📤 Send 1: {patient['patient_id']} — {patient['name']}")
        time.sleep(0.5)

        # Duplicate send — same patient again!
        producer.send('patient-admissions', key=patient['patient_id'], value=patient)
        print(f"📤 Send 2 (DUPLICATE): {patient['patient_id']} — {patient['name']}")
        time.sleep(0.5)

    producer.flush()
    producer.close()
    print(f"\n✅ Sent 10 patients × 2 = 20 messages to Kafka")
    print(f"⚠️  Your database now has duplicate records!")
    print(f"🔍 Run the deduplication script to fix it!")

if __name__ == "__main__":
    simulate_duplicates()