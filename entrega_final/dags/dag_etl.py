from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import main

default_args = {
    'owner': 'Gabriel Delgado',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
    }

with DAG(
    dag_id = 'dag_crypto_currency',
    description='DAG para consultar y almacenar valores de criptomonedas',
    start_date = datetime(2023, 12, 4),
    catchup = False,
    schedule_interval = '@daily',
    default_args = default_args
    ) as dag:

    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'redshift',
        sql = 'sql/create_table.sql',
        hook_params = {'options': '-c search_path=gaby_delgado11_coderhouse'}
        )
    
    main_task = PythonOperator(
        task_id = 'main_task',
        python_callable = main,
        op_kwargs = {'config': '/opt/airflow/config/config.ini'}
        )
    
    create_table >> main_task
