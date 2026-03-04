from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import psycopg2
import json
import logging

# ------------------------------------
# Default DAG arguments
# ------------------------------------
default_args = {
    'owner': 'mediflow-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,                           # retry 3 times if fails
    'retry_delay': timedelta(minutes=5),    # wait 5 mins between retries
}

# ------------------------------------
# Database connection
# ------------------------------------
def get_db():
    return psycopg2.connect(
        host="mediflow_postgres",  # container name — not localhost!
        port=5432,
        database="mediflow_db",
        user="mediflow",
        password="mediflow123"
    )

# ------------------------------------
# Task 1: Check Data Quality
# ------------------------------------
def check_data_quality(**context):
    logging.info("🔍 Starting data quality check...")
    conn = get_db()
    cursor = conn.cursor()

    issues = []

    # Check for null patient IDs
    cursor.execute("SELECT COUNT(*) FROM patients WHERE patient_id IS NULL")
    null_ids = cursor.fetchone()[0]
    if null_ids > 0:
        issues.append(f"Found {null_ids} patients with NULL patient_id")

    # Check for invalid ages
    cursor.execute("SELECT COUNT(*) FROM patients WHERE age < 0 OR age > 150")
    invalid_ages = cursor.fetchone()[0]
    if invalid_ages > 0:
        issues.append(f"Found {invalid_ages} patients with invalid age")

    # Check for missing diagnoses
    cursor.execute("SELECT COUNT(*) FROM patients WHERE diagnosis IS NULL")
    missing_diag = cursor.fetchone()[0]
    if missing_diag > 0:
        issues.append(f"Found {missing_diag} patients with missing diagnosis")

    # Check total records
    cursor.execute("SELECT COUNT(*) FROM patients")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM lab_results")
    total_labs = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    # Push results to XCom (Airflow's way of passing data between tasks)
    context['ti'].xcom_push(key='quality_issues', value=issues)
    context['ti'].xcom_push(key='total_patients', value=total)
    context['ti'].xcom_push(key='total_labs', value=total_labs)

    if issues:
        logging.warning(f"⚠️ Data quality issues found: {issues}")
    else:
        logging.info(f"✅ Data quality check passed! {total} patients, {total_labs} lab results")

    return {"total_patients": total, "total_labs": total_labs, "issues": issues}

# ------------------------------------
# Task 2: Generate Summary Statistics
# ------------------------------------
def generate_statistics(**context):
    logging.info("📊 Generating hospital statistics...")
    conn = get_db()
    cursor = conn.cursor()

    # Patients by department
    cursor.execute("""
        SELECT department, COUNT(*) as count
        FROM patients
        GROUP BY department
        ORDER BY count DESC
    """)
    dept_stats = cursor.fetchall()

    # Patients by severity
    cursor.execute("""
        SELECT severity, COUNT(*) as count
        FROM patients
        GROUP BY severity
        ORDER BY count DESC
    """)
    severity_stats = cursor.fetchall()

    # Average vitals
    cursor.execute("""
        SELECT
            ROUND(AVG(heart_rate), 1) as avg_heart_rate,
            ROUND(AVG(oxygen_saturation), 1) as avg_oxygen,
            ROUND(AVG(temperature), 1) as avg_temp
        FROM patients
        WHERE heart_rate IS NOT NULL
    """)
    vitals = cursor.fetchone()

    cursor.close()
    conn.close()

    stats = {
        "departments": dict(dept_stats),
        "severities": dict(severity_stats),
        "avg_vitals": {
            "heart_rate": float(vitals[0]) if vitals[0] else 0,
            "oxygen_saturation": float(vitals[1]) if vitals[1] else 0,
            "temperature": float(vitals[2]) if vitals[2] else 0
        }
    }

    context['ti'].xcom_push(key='statistics', value=stats)
    logging.info(f"📊 Statistics generated: {json.dumps(stats, indent=2)}")
    return stats

# ------------------------------------
# Task 3: Flag Critical Patients
# ------------------------------------
def flag_critical_patients(**context):
    logging.info("🚨 Checking for critical patients...")
    conn = get_db()
    cursor = conn.cursor()

    # Find critical patients with dangerous vitals
    cursor.execute("""
        SELECT
            patient_id,
            name,
            department,
            heart_rate,
            oxygen_saturation,
            temperature,
            severity
        FROM patients
        WHERE
            severity = 'Critical'
            OR oxygen_saturation < 90
            OR heart_rate > 120
            OR heart_rate < 40
            OR temperature > 103
        ORDER BY severity DESC
        LIMIT 20
    """)
    critical = cursor.fetchall()

    cursor.close()
    conn.close()

    critical_list = []
    for p in critical:
        critical_list.append({
            "patient_id": p[0],
            "name": p[1],
            "department": p[2],
            "heart_rate": p[3],
            "oxygen_saturation": p[4],
            "temperature": float(p[5]) if p[5] else None,
            "severity": p[6]
        })

    context['ti'].xcom_push(key='critical_patients', value=critical_list)

    if critical_list:
        logging.warning(f"🚨 Found {len(critical_list)} critical patients!")
        for p in critical_list[:5]:
            logging.warning(f"   CRITICAL: {p['name']} | "
                          f"HR: {p['heart_rate']} | "
                          f"O2: {p['oxygen_saturation']}% | "
                          f"Dept: {p['department']}")
    else:
        logging.info("✅ No critical patients found")

    return critical_list

# ------------------------------------
# Task 4: Create Daily Summary
# ------------------------------------
def create_daily_summary(**context):
    logging.info("📋 Creating daily summary report...")
    ti = context['ti']

    # Pull results from previous tasks using XCom
    quality = ti.xcom_pull(task_ids='check_data_quality', key='quality_issues')
    total_patients = ti.xcom_pull(task_ids='check_data_quality', key='total_patients')
    total_labs = ti.xcom_pull(task_ids='check_data_quality', key='total_labs')
    stats = ti.xcom_pull(task_ids='generate_statistics', key='statistics')
    critical = ti.xcom_pull(task_ids='flag_critical_patients', key='critical_patients')

    summary = f"""
    ==========================================
    🏥 MEDIFLOW DAILY SUMMARY REPORT
    ==========================================
    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    📊 OVERVIEW
    Total Patients:     {total_patients}
    Total Lab Results:  {total_labs}
    Critical Patients:  {len(critical) if critical else 0}
    Quality Issues:     {len(quality) if quality else 0}

    🏥 PATIENTS BY DEPARTMENT
    {chr(10).join([f"    {dept}: {count}" for dept, count in (stats.get('departments', {}) if stats else {}).items()])}

    🚨 SEVERITY BREAKDOWN
    {chr(10).join([f"    {sev}: {count}" for sev, count in (stats.get('severities', {}) if stats else {}).items()])}

    💊 AVERAGE VITALS
    Heart Rate:         {stats.get('avg_vitals', {}).get('heart_rate', 'N/A') if stats else 'N/A'} bpm
    Oxygen Saturation:  {stats.get('avg_vitals', {}).get('oxygen_saturation', 'N/A') if stats else 'N/A'}%
    Temperature:        {stats.get('avg_vitals', {}).get('temperature', 'N/A') if stats else 'N/A'}°F

    ==========================================
    """

    logging.info(summary)
    print(summary)
    return summary

# ------------------------------------
# Define the DAG
# ------------------------------------
with DAG(
    dag_id='mediflow_pipeline',
    default_args=default_args,
    description='MediFlow Healthcare Data Pipeline',
    schedule_interval='@hourly',        # runs every hour
    catchup=False,
    tags=['healthcare', 'mediflow']
) as dag:

    # Define tasks
    task_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True
    )

    task_stats = PythonOperator(
        task_id='generate_statistics',
        python_callable=generate_statistics,
        provide_context=True
    )

    task_critical = PythonOperator(
        task_id='flag_critical_patients',
        python_callable=flag_critical_patients,
        provide_context=True
    )

    task_summary = PythonOperator(
        task_id='create_daily_summary',
        python_callable=create_daily_summary,
        provide_context=True
    )

    # Define task order — this is the DAG structure!
    task_quality >> task_stats >> task_critical >> task_summary