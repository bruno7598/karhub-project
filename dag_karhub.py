from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os

csv_directory = '/opt/airflow/dags/karhub_project/resources/csvs/'
feather_directory = '/opt/airflow/dags/karhub_project/resources/feather/'

def csv_to_feather(csv_filename):
    csv_path = os.path.join(csv_directory, csv_filename)
    feather_filename = os.path.splitext(csv_filename)[0] + '.feather'
    feather_path = os.path.join(feather_directory, feather_filename)
    
    df = pd.read_csv(csv_path, encoding='latin1')
    df.to_feather(feather_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 30),
    'retries': 1,
}

dag = DAG(
    'csv_to_feather_task',
    default_args=default_args,
    description='Task para ler CSV e salvar como Feather',
    schedule_interval=None,
)

csv_files = ['gdvDespesasExcel.csv', 'gdvReceitasExcel.csv'] 


for csv_file in csv_files:
    task_id = f'csv_to_feather_task_{csv_file.replace(".csv", "")}'
    csv_to_feather_task = PythonOperator(
        task_id=task_id,
        python_callable=csv_to_feather,
        op_args=[csv_file],
        dag=dag,
    )

csv_to_feather_task

DAGS = [dag]