from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import tempfile
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_csv_from_minio():
    """Read the CSV file from MinIO and process it"""
    hook = S3Hook(aws_conn_id='minio_connection')
    
    # Download the CSV file
    file_content = hook.read_key(key='sample_data.csv', bucket_name='raw-data')
    
    # Save content to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(file_content)
        temp_path = f.name
    
    try:
        # Read CSV with pandas
        df = pd.read_csv(temp_path)
        print("📊 ORIGINAL DATA:")
        print(df)
        
        # Basic processing - calculate statistics
        total_value = df['value'].sum()
        average_value = df['value'].mean()
        max_value = df['value'].max()
        min_value = df['value'].min()
        
        print(f"\n📈 STATISTICS:")
        print(f"Total value: {total_value}")
        print(f"Average value: {average_value:.2f}")
        print(f"Max value: {max_value}")
        print(f"Min value: {min_value}")
        
        # Create processed data
        processed_data = {
            'timestamp': datetime.now().isoformat(),
            'total_value': int(total_value),
            'average_value': float(average_value),
            'max_value': int(max_value),
            'min_value': int(min_value),
            'row_count': len(df),
            'original_file': 'sample_data.csv'
        }
        
        return processed_data
        
    finally:
        os.unlink(temp_path)

def save_processed_data(processed_data):
    """Save processed results back to MinIO"""
    hook = S3Hook(aws_conn_id='minio_connection')
    
    # Convert to DataFrame for CSV output
    results_df = pd.DataFrame([processed_data])
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        results_df.to_csv(f.name, index=False)
        temp_path = f.name
    
    try:
        # Read the file content and upload
        with open(temp_path, 'r') as f:
            file_content = f.read()
        
        hook.load_string(
            string_data=file_content,
            key='processed/results.csv',
            bucket_name='raw-data',
            replace=True
        )
        print("✅ Processed results saved to MinIO!")
        
    finally:
        os.unlink(temp_path)

def list_minio_files():
    """List all files in MinIO buckets"""
    hook = S3Hook(aws_conn_id='minio_connection')
    
    print("📁 FILES IN MINIO:")
    
    try:
        raw_files = hook.list_keys(bucket_name='raw-data')
        print(f"Raw data bucket: {list(raw_files) if raw_files else 'No files'}")
    except Exception as e:
        print(f"Error listing raw-data: {e}")

# Define the DAG
with DAG(
    'process_csv_pipeline',
    default_args=default_args,
    description='A simple pipeline to process CSV data from MinIO',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['minio', 'csv', 'tutorial']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    process_task = PythonOperator(
        task_id='process_csv_data',
        python_callable=read_csv_from_minio
    )
    
    save_task = PythonOperator(
        task_id='save_processed_results',
        python_callable=save_processed_data,
        op_args=["{{ ti.xcom_pull(task_ids='process_csv_data') }}"]
    )
    
    list_files_task = PythonOperator(
        task_id='list_minio_files',
        python_callable=list_minio_files
    )
    
    end = DummyOperator(task_id='end')
    
    # Define workflow
    start >> process_task >> save_task >> list_files_task >> end
