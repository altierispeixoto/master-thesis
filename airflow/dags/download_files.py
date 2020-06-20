from airflow.models import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
import yaml

from lib.utils import download_files , decompress_files , delete_files

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)

date_range = ast.literal_eval(Variable.get("date_range"))
base_url = Variable.get("base_url")

args = {
    'owner': 'airflow',
    'description': 'Download files for processing',
    'depend_on_past': False,
    'start_date':  airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'pool': 'download-files',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='download_files', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
wait = DummyOperator(task_id='wait', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

download_tasks = []
decompress_tasks = []
delete_staging_files = []

for t in config['etl_tasks']:
    config['etl_tasks'][t]['date_range'] = date_range
    config['etl_tasks'][t]['base_url'] = base_url

    download_tasks.append(PythonOperator(
        task_id=f"download_{t}",
        provide_context=True,
        op_kwargs=config['etl_tasks'][t],

        python_callable=download_files,
        dag=dag,
    ))

    decompress_tasks.append(PythonOperator(
        task_id=f"decompress_to_raw_{t}",
        provide_context=True,
        op_kwargs=config['etl_tasks'][t],
        python_callable=decompress_files,
        dag=dag,
    ))


delete_staging_files = PythonOperator(
        task_id= "delete_staging_files",
        provide_context=True,
        python_callable=delete_files,
        dag=dag,
    )

for j in range(0, len(download_tasks)):
    start >> download_tasks[j] >> decompress_tasks[j] >> wait

wait >> delete_staging_files >> end