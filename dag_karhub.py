import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from settings import Settings
from routines.raw import Raw
from routines.conformed import Conformed
from routines.application import Application

env = Variable.get('mode_deploy')

settings = Settings()
settings.set_env(env)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 30),
    'retries': 1,
}

dag = DAG(
    'karhub_project',
    default_args=default_args,
    description='Task para ler CSV e salvar como Feather',
    schedule_interval=None,
)

files = ['gdvDespesasExcel', 'gdvReceitasExcel']

for file in files:
    task_id = f'csv_to_feather_task_raw_{file}'
    csv_to_feather_task = PythonOperator(
        task_id=task_id,
        python_callable=Raw().routine_raw,
        op_args=[file],
        dag=dag,
    )


    read_feather_task = PythonOperator(
        task_id=f'read_feather_task_conformed_{file}',
        python_callable=Conformed().routines_conformed,
        op_args=[file],
        dag=dag,
    )


    insert_data_task = PythonOperator(
        task_id=f'insert_data_task_application_{file}',
        python_callable=Application().routines_application,
        op_args=[file],
        dag=dag,
    )

    csv_to_feather_task >> read_feather_task >> insert_data_task

DAGS = [dag]
