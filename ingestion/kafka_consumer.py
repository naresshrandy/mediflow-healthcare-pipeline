import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# ------------------------------------
# Database connection
# ------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="mediflow_db",
        user="mediflow",
        password="mediflow123"
    )

# ------------------------------------
# Data Quality Validator
# 🔴 Challenge 1 — Data Quality Crisis
# ------------------------------------
class DataQualityValidator:
    
    REQUIRED_PATIENT_FIELDS = [
        'patient_id', 'name', 'age', 'gender',
        'department', 'diagnosis', 'severity'
    ]
    
    REQUIRED_LAB_FIELDS = [
        'lab_id', 'patient_id', 'test_name',
        'result_value', 'status'
    ]

    def validate_patient(self, record):
        issues = []

        # Check required fields
        for field in self.REQUIRED_PATIENT_FIELDS:
            if field not in record or record[field] is None:
                issues.append(f"Missing required field: {field}")

        # Check age is valid
        if 'age' in record:
            if not isinstance(record['age'], int) or record['age'] < 0 or record['age'] > 150:
                issues.append(f"Invalid age: {record['age']}")

        # Check severity is valid
        valid_severities = ['Low', 'Medium', 'High', 'Critical']
        if 'severity' in record and record['severity'] not in valid_severities:
            issues.append(f"Invalid severity: {record['severity']}")

        # Check vitals exist
        if 'vitals' not in record:
            issues.append("Missing vitals data")
        else:
            vitals = record['vitals']
            if vitals.get('heart_rate', 0) < 20 or vitals.get('heart_rate', 0) > 250:
                issues.append(f"Invalid heart rate: {vitals.get('heart_rate')}")
            if vitals.get('oxygen_saturation', 0) < 50:
                issues.append(f"Critical oxygen saturation: {vitals.get('oxygen_saturation')}")

        return issues

    def validate_lab(self, record):
        issues = []

        for field in self.REQUIRED_LAB_FIELDS:
            if field not in record or record[field] is None:
                issues.append(f"Missing required field: {field}")

        valid_statuses = ['Normal', 'Abnormal', 'Critical']
        if 'status' in record and record['status'] not in valid_statuses:
            issues.append(f"Invalid status: {record['status']}")

        return issues

# ------------------------------------
# Save patient to PostgreSQL
# ------------------------------------
def save_patient(conn, record):
    cursor = conn.cursor()
    try:
        vitals = record.get('vitals', {})
        cursor.execute("""
            INSERT INTO patients (
                patient_id, name, age, gender, blood_type,
                department, diagnosis, severity, doctor,
                admission_time, heart_rate, blood_pressure_systolic,
                blood_pressure_diastolic, temperature,
                oxygen_saturation, respiratory_rate,
                insurance, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (patient_id) DO NOTHING
        """, (
            record.get('patient_id'),
            record.get('name'),
            record.get('age'),
            record.get('gender'),
            record.get('blood_type'),
            record.get('department'),
            record.get('diagnosis'),
            record.get('severity'),
            record.get('doctor'),
            record.get('admission_time'),
            vitals.get('heart_rate'),
            vitals.get('blood_pressure_systolic'),
            vitals.get('blood_pressure_diastolic'),
            vitals.get('temperature'),
            vitals.get('oxygen_saturation'),
            vitals.get('respiratory_rate'),
            record.get('insurance'),
            record.get('created_at')
        ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving patient: {e}")
        return False
    finally:
        cursor.close()

# ------------------------------------
# Save lab result to PostgreSQL
# ------------------------------------
def save_lab_result(conn, record):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO lab_results (
                lab_id, patient_id, test_name,
                result_value, unit, status,
                lab_technician, collected_at, resulted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (lab_id) DO NOTHING
        """, (
            record.get('lab_id'),
            record.get('patient_id'),
            record.get('test_name'),
            record.get('result_value'),
            record.get('unit'),
            record.get('status'),
            record.get('lab_technician'),
            record.get('collected_at'),
            record.get('resulted_at')
        ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving lab result: {e}")
        return False
    finally:
        cursor.close()

# ------------------------------------
# Log data quality issues
# ------------------------------------
def log_quality_issue(conn, record_id, record_type, issues, raw_data):
    cursor = conn.cursor()
    try:
        for issue in issues:
            cursor.execute("""
                INSERT INTO data_quality_log (
                    record_id, record_type,
                    issue_type, issue_description, raw_data
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                record_id,
                record_type,
                "VALIDATION_ERROR",
                issue,
                json.dumps(raw_data)
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ Error logging quality issue: {e}")
    finally:
        cursor.close()

# ------------------------------------
# Main Consumer
# ------------------------------------
def start_consumer():
    print("🔄 MediFlow Kafka Consumer Starting...")
    print("📡 Connecting to Kafka and PostgreSQL...")

    validator = DataQualityValidator()
    conn = get_db_connection()
    print("✅ Connected to PostgreSQL!")

    consumer = KafkaConsumer(
        'patient-admissions',
        'lab-results',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='mediflow-consumer-group',
        auto_offset_reset='earliest'
    )
    print("✅ Connected to Kafka!")
    print("👂 Listening for patient data...\n")

    patients_saved = 0
    labs_saved = 0
    quality_issues = 0

    try:
        for message in consumer:
            topic = message.topic
            record = message.value

            if topic == 'patient-admissions':
                issues = validator.validate_patient(record)

                if issues:
                    quality_issues += 1
                    log_quality_issue(
                        conn,
                        record.get('patient_id', 'UNKNOWN'),
                        'PATIENT',
                        issues,
                        record
                    )
                    print(f"⚠️  Quality issue for {record.get('patient_id')}: {issues}")
                else:
                    if save_patient(conn, record):
                        patients_saved += 1
                        print(f"✅ Patient saved: {record.get('name')} | "
                              f"Dept: {record.get('department')} | "
                              f"Severity: {record.get('severity')} | "
                              f"Total: {patients_saved}")

            elif topic == 'lab-results':
                issues = validator.validate_lab(record)

                if issues:
                    quality_issues += 1
                    log_quality_issue(
                        conn,
                        record.get('lab_id', 'UNKNOWN'),
                        'LAB',
                        issues,
                        record
                    )
                else:
                    if save_lab_result(conn, record):
                        labs_saved += 1
                        print(f"🧪 Lab saved: {record.get('test_name')} | "
                              f"Status: {record.get('status')} | "
                              f"Total: {labs_saved}")

    except KeyboardInterrupt:
        print(f"\n⏹️  Consumer stopped")
        print(f"📊 Final stats:")
        print(f"   Patients saved:  {patients_saved}")
        print(f"   Labs saved:      {labs_saved}")
        print(f"   Quality issues:  {quality_issues}")
    finally:
        consumer.close()
        conn.close()
        print("✅ Connections closed cleanly")

if __name__ == "__main__":
    start_consumer()