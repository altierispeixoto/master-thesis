import ast
from datetime import timedelta

import airflow
import yaml
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from lib.utils import download_files, decompress_files, delete_files, create_task

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)

date_range = ast.literal_eval(Variable.get("date_range"))
base_url = Variable.get("base_url")

args = {
    'owner': 'airflow',
    'description': 'Download files for processing',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'pool': 'download-files',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='raw-ingestion', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
wait = DummyOperator(task_id='wait', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

download_tasks = []
decompress_tasks = []
delete_staging_files = []


for t in config['etl_tasks']:
    config['etl_tasks'][t]['date_range'] = date_range
    config['etl_tasks'][t]['base_url'] = base_url

    download_tasks.append(create_task(f"download_{t}", config['etl_tasks'][t], download_files, dag))
    decompress_tasks.append(create_task(f"decompress_to_raw_{t}", config['etl_tasks'][t], decompress_files, dag))

delete_staging_files = create_task("delete_staging_files", None, delete_files, dag)

for j in range(0, len(download_tasks)):
    start >> download_tasks[j] >> decompress_tasks[j] >> wait

wait >> delete_staging_files >> end
