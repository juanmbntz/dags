from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
import csv
import requests
import json
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np
import time
import requests
import re
from datetime import datetime
import os
from scripts.scrapper_prices_test import scrap_prices

dag = DAG(
    dag_id="scrapper_supermercado_test",
    description="Scrapper para precios de supermercado",
    max_active_runs=1,
    catchup=True,
    start_date=datetime(2022, 5, 21, 0 ,0),
    schedule_interval="0 0 * * *",
    default_args={
        "owner": "jmbenitez",
        "email": "juanmbntz@gmail.com",
        "depends_on_past": True,
    },
)

start_task = DummyOperator(
    task_id = 'start',
    dag=dag,
    wait_for_downstream=True
)

scraping_prices = PythonOperator(
    task_id='scrapping_prices',
    dag=dag,
    python_callable=scrap_prices
)


send_email = EmailOperator(
        task_id="send_email",
        dag=dag,
        to=["juanmbntz@gmail.com"],
        subject="data_pipeline_supermercado",
        files=["/home/bjuanm/airflow/dags/files/precios_coto_test.csv"],
        html_content=f" <h3>Corrida del dia {datetime.today().strftime('%Y-%m-%d')}</h3>"
)

push_to_github = BashOperator(
    task_id="push_files",
    dag=dag,
    bash_command="""
                cd /home/bjuanm/airflow/dags/ &&\
                git add . &&\
                git commit -m "push del dia" &&\
                git push origin master &&\
                expect "Username for 'https://github.com':" &&\
                send --"juanmbntz\r" &&\
                expect "Password for 'https://juanmbntz@github.com':" &&\
                send --"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\r" &&\
                interact
"""
)

start_task >> scraping_prices >> send_email >> push_to_github