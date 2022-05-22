from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="alkemy_universidades",
    description="Query para universidades",
    max_active_runs=1,
    start_date=datetime(2022, 3, 13, 0, 0),
    schedule_interval="@weekly",
)

query_db = PostgresOperator(
    task_id="query_table",
    dag=dag,
    postgres_conn_id="conn_universities",
    sql=""" SELECT 
                    universidad AS university,
                    carrera AS career,
                    fecha_de_inscripcion AS inscription_date,
                    SPLIT_PART(name, ' ', 1) AS first_name,
                    SPLIT_PART(name, ' ', 2) AS last_name,
                    sexo AS gender,
                    codigo_postal AS codigo_postal,
                    correo_electronico AS email

                FROM flores_comahue 
                WHERE universidad = 'UNIVERSIDAD DE FLORES' """
)

query_db