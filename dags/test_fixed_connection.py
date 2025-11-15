from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_minio_connection():
    """Test connection with host.docker.internal"""
    hook = S3Hook(aws_conn_id='minio_connection')
    
    try:
        files = hook.list_keys(bucket_name='raw-data')
        print(f"✅ Connected successfully!")
        print(f"📁 Files in raw-data bucket: {list(files) if files else 'No files'}")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

with DAG(
    'test_fixed_connection',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_minio_connection
    )
