from datetime import datetime
import random
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

OUTPUT_PATH = "/opt/airflow/dags/digital_wallet_transactions.csv"

# ==================== 1. EXTRACT TASK ====================
def generate_and_save_wallet_data():
    print("--- STARTING DIGITAL WALLET DATA EXTRACTION ---")
    payment_modes = ["Wallet Transfer", "QR Payment", "Bank Transfer", "Utility Bill Pay"]
    statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]
    
    transactions = []
    for _ in range(5):
        tx_id = f"TXN-{random.randint(100000, 999999)}"
        amount = random.randint(50, 15000)
        mode = random.choice(payment_modes)
        status = random.choice(statuses)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        transactions.append([tx_id, amount, mode, status, timestamp])
    
    file_exists = os.path.isfile(OUTPUT_PATH)
    with open(OUTPUT_PATH, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Transaction_ID", "Amount_NPR", "Payment_Mode", "Status", "Timestamp"])
        writer.writerows(transactions)
    print("Raw data successfully written to CSV.")

# ==================== 2. TRANSFORM TASK ====================
def transform_and_calculate_metrics(**context):
    print("--- STARTING DIGITAL WALLET DATA TRANSFORMATION ---")
    
    if not os.path.isfile(OUTPUT_PATH):
        print("No transaction file found to transform.")
        return

    total_success_volume = 0
    success_count = 0
    failed_count = 0
    
    with open(OUTPUT_PATH, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Status'] == 'SUCCESS':
                total_success_volume += int(row['Amount_NPR'])
                success_count += 1
            else:
                failed_count += 1
                
    # Push data into Airflow's memory sharing system (XComs) so the Load task can access it
    context['ti'].xcom_push(key='success_count', value=success_count)
    context['ti'].xcom_push(key='failed_count', value=failed_count)
    context['ti'].xcom_push(key='total_volume', value=total_success_volume)
    
    print("Transformation completed and metrics pushed to memory.")

# ==================== 3. LOAD TASK ====================
def load_metrics_to_postgres(**context):
    print("--- STARTING SQL DATABASE LOAD STAGE ---")
    import psycopg2 # Imports the native PostgreSQL python driver
    
    # Pull metrics generated in the previous transformation task
    ti = context['ti']
    success_count = ti.xcom_pull(key='success_count', task_ids='transform_and_aggregate')
    failed_count = ti.xcom_pull(key='failed_count', task_ids='transform_and_aggregate')
    total_volume = ti.xcom_pull(key='total_volume', task_ids='transform_and_aggregate')
    execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Direct raw connection string setup
    production_db_uri = "postgresql://airflow:airflow@postgres:5432/airflow"
    
    # Connect directly via native psycopg2 driver
    conn = psycopg2.connect(production_db_uri)
    cursor = conn.cursor()
    
    # 1. Create the table if it does not exist yet (Anonymized Table Name)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS wallet_batch_metrics (
        id SERIAL PRIMARY KEY,
        run_timestamp TIMESTAMP,
        successful_transactions INT,
        failed_transactions INT,
        total_volume_npr INT
    );
    """
    cursor.execute(create_table_sql)
    
    # 2. Insert the metric rows into the SQL database
    insert_sql = """
    INSERT INTO wallet_batch_metrics (run_timestamp, successful_transactions, failed_transactions, total_volume_npr)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_sql, (execution_date, success_count, failed_count, total_volume))
    
    # Commit changes and close connection cleanly
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Successfully committed aggregated pipeline metrics to PostgreSQL database table 'wallet_batch_metrics'!")

# ==================== DAG SCHEDULING ====================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='digital_wallet_transaction_pipeline',  # Updated DAG ID
    default_args=default_args,
    description='Full end-to-end production ETL pipeline storing data into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['production_wallet_etl']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=generate_and_save_wallet_data,
    )

    transform_task = PythonOperator(
        task_id='transform_and_aggregate',
        python_callable=transform_and_calculate_metrics,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_into_sql',
        python_callable=load_metrics_to_postgres,
        provide_context=True,
    )

    # The production lineage flow: Extract -> Transform -> Load
    extract_task >> transform_task >> load_task
