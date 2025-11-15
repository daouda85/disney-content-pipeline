from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_minio_connection():
    """Test if we can connect to MinIO and list files"""
    hook = S3Hook(aws_conn_id='minio_connection')
    
    try:
        # Check connection by listing files in the bucket
        files = hook.list_keys(bucket_name='raw-data')
        print(f"✅ Connected successfully!")
        print(f"📁 Files in raw-data bucket: {list(files) if files else 'No files'}")
        
        # If you want to check if bucket exists (alternative method)
        if hook.check_for_bucket('raw-data'):
            print(f"✅ Bucket 'raw-data' exists!")
        else:
            print(f"❌ Bucket 'raw-data' does not exist!")
        
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

with DAG(
    'test_minio_connection_fixed',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_minio_connection
    )
