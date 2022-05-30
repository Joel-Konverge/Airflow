import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


from datetime import datetime
import json

default_args={"start_date":datetime(2022,5,5)}

def _process_datetime(ti):
    dt=ti.xcom_pull(task_ids=['get_datetime'])
    if not dt:
        raise Exception('No datetime value.')
    dt=str(dt[0]).split()
    return{
        'year':int(dt[3]),
        'month':dt[2],
        'day':int(dt[1]),
        'time':dt[4],
        'day_of_week':dt[0]
    }

def _save_datetime(ti):
    dt_processed=ti.xcom_pull(task_ids=['process_datetime'])
    if not dt_processed:
        raise Exception('No processed datetime value.')
    df=pd.DataFrame(dt_processed)
    csv_path=Variable.get('test1_csv_path')
    if os.path.exists(csv_path):
        df_header=False
        df_mode='a'
    else:
        df_header=True
        df_mode='w'
    df.to_csv(csv_path,index=False,mode=df_mode,header=df_header)

with DAG('test1',schedule_interval='@daily',start_date=datetime(2022,5,5),catchup=False) as dag:
    task_get_datetime=BashOperator(
    task_id='get_datetime',
    bash_command='date'
    )

    task_process_datetime=PythonOperator(
    task_id='process_datetime',
    python_callable=_process_datetime
    )

    task_save_datetime=PythonOperator(
    task_id='save_datetime',
    python_callable=_save_datetime
    )


task_get_datetime >> task_process_datetime >> task_save_datetime