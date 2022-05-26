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
from scripts.scrapper_prices_bda import scrap_prices
from scripts.calc_increm_bda import calc_increment

dag = DAG(
    dag_id="scrapper_bda",
    description="Scrapper para precios de supermercado - lista de bda",
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2022, 5, 25, 0 ,0),
    schedule_interval="30 0 * * *",
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

calc_increm = PythonOperator(
    task_id='calc_increm',
    dag=dag,
    python_callable=calc_increment
)



send_email = EmailOperator(
        task_id="send_email",
        dag=dag,
        to=["juanmbntz@gmail.com"],#,"laralopezcalvo13@gmail.com"],
        subject="precios_bda_incrementos",
        files=["/home/bjuanm/airflow/dags/files/precios_coto_increm_bda.csv"],
        html_content=f" <h3>Corrida del dia {datetime.today().strftime('%Y-%m-%d')}</h3>"
)

start_task >> scraping_prices >> calc_increm >> send_email 