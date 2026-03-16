from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for Airflow/Composer',
    schedule_interval='@daily',
    start_date=datetime(2025, 6, 1),
    catchup=False,
) as dag:

    # Task 1: Start (Dummy task)
    start = DummyOperator(task_id='start')

    # Task 2: Extract data (Python function)
    def extract_data():
        # Simulate data extraction (e.g., from an API or file)
        print("Extracting data...")
        return [{"id": 1, "value": "data1"}, {"id": 2, "value": "data2"}]

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    # Task 3: Transform data (Python function)
    def transform_data():
        # Simulate transformation
        print("Transforming data...")
        # In practice, you'd process the extracted data here
        return "Transformed data"

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 4: Load to BigQuery (Composer-compatible)
    load_to_bq = BigQueryOperator(
        task_id='load_to_bigquery',
        bql='SELECT * FROM `project.dataset.table` WHERE 1=0',  # Placeholder query
        destination_dataset_table='project.dataset.transformed_data',
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False,
    )

    # Task 5: End (Dummy task)
    end = DummyOperator(task_id='end')

    # Define dependencies
    start >> extract >> transform >> load_to_bq >> end
