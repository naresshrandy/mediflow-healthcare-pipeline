import psycopg2

def get_db():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="mediflow_db",
        user="mediflow",
        password="mediflow123"
    )

def find_duplicates():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT patient_id, COUNT(*) as count
        FROM patients
        GROUP BY patient_id
        HAVING COUNT(*) > 1
    """)
    
    results = cursor.fetchall()
    
    for row in results:
        print(row)
    
    conn.close()

find_duplicates()