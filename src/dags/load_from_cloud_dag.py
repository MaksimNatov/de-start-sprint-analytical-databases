import boto3
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

AWS_ACCESS_KEY_ID = "Вставь AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "Вставь AWS_SECRET_ACCESS_KEY"

bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']
bucket_files = [f'/data/{f}' for f in bucket_files]
BUCKET_STR = ' '.join(bucket_files)

def upload_to_data(filename: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket='sprint6',
        Key=filename,
        Filename=f'/data/{filename}'
    )


bash_command_tmpl = """
head -n 10 {{ params.files }}
"""

with DAG(
        'upload_to_data',
        # Задаем расписание выполнения дага
        schedule_interval=None,
        description='upload_to_data',
        catchup=False,
        start_date=datetime.today()
) as dag:

    upload_groups = PythonOperator(
        task_id='upload_groups',
        python_callable=upload_to_data,
        op_kwargs={
            'filename': 'groups.csv'
        },
    )

    upload_users = PythonOperator(
        task_id='upload_users',
        python_callable=upload_to_data,
        op_kwargs={
            'filename': 'users.csv'
        },
    )
    upload_dialogs = PythonOperator(
        task_id='upload_dialogs',
        python_callable=upload_to_data,
        op_kwargs={
            'filename': 'dialogs.csv'
        },
    )

    upload_dialogs = PythonOperator(
        task_id='upload_group_log',
        python_callable=upload_to_data,
        op_kwargs={
            'filename': 'group_log.csv'
        },
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': BUCKET_STR}
        
    )

    ([upload_groups, upload_users, upload_dialogs] 
     >> print_10_lines_of_each)
