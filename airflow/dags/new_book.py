from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json

from datetime import datetime
import os
import pandas as pd

default_args={"start_date":datetime(2022,5,5)}

def _process_books(ti):
    books=ti.xcom_pull(task_ids=["get_books"])
    if not books:
        raise Exception("Book not found")
    data=books[0]
    books_list=[]
    for i in data["books"]:
        books_list.append({"Book Title":i["title"],"ISBN No.":i["isbn13"],"Price":i["price"],"Book Image":i["image"],"Book URL":i["url"]})
    return books_list


def _save_books(ti):
    books_data=ti.xcom_pull(task_ids=["process_books"])
    if not books_data:
        raise Exception("Books not found")
    csv_path=Variable.get('books_csv_path')
    if os.path.exists(csv_path):
        df_header=False
        df_mode='a'
    else:
        df_header=True
        df_mode='w'
    for i in books_data:
        df=pd.DataFrame(i)
        df.to_csv(csv_path,index=False,mode=df_mode,header=df_header)    


with DAG(dag_id="new_book",schedule_interval="@daily",default_args=default_args,catchup=False) as dag:
    check_api=HttpSensor(
        task_id="check_api",
        http_conn_id="book_api",
        endpoint="new"
    )

    get_books=SimpleHttpOperator(
        task_id="get_books",
        http_conn_id="book_api",
        endpoint="new",
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    process_books=PythonOperator(
        task_id="process_books",
        python_callable=_process_books
    )

    save_books=PythonOperator(
        task_id="save_books",
        python_callable=_save_books
    )

check_api >> get_books >> process_books >> save_books