from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

def helloworld():
    print("Hello World")

with DAG("hello",start_date=datetime(2022,5,5),schedule_interval="@daily",catchup=False) as dag:
    task = PythonOperator(
    task_id="hello_world",
    python_callable=helloworld)

task