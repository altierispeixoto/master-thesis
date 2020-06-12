from airflow.models import DAG
import airflow
from pprint import pprint
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
import urllib3
import lzma
import yaml
import os
import shutil

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)


def download_files(ds, folder, file, **kwargs):
    pprint(f"Date range: {date_range}")
    pprint(f"Base URL: {base_url}")
    pprint(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        url = f"{base_url}{download_file_day}_{file}"
        pprint(f"Downloading: {url}")

        base_folder = f"data/staging/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        fd = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"

        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)

        with open(fd, 'wb') as out:
            while True:
                data = r.read()
                if not data:
                    break
                out.write(data)

        r.release_conn()

    return "download realizado com sucesso"


def decompress_files(ds, folder, file, **kwargs):
    pprint(f"Date range: {date_range}")
    pprint(f"Base URL: {base_url}")
    pprint(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        base_folder = f"data/raw/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        try:
            fstaging = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"
            fraw = f"{base_folder}/{download_file_day}_{file.replace('.xz', '')}"

            binary_data_buffer = lzma.open(fstaging, mode='rt', encoding='utf-8').read()

            with open(fraw, 'w') as a:
                a.write(binary_data_buffer)

        except Exception as err:
            print(f"Can't open file: {file} for date {download_file_day}")


def delete_files(ds, **kwargs):
    base_folder = f"data/staging/"
    shutil.rmtree(base_folder, ignore_errors=True)


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
    task_id="delete_staging_files",
    provide_context=True,
    python_callable=delete_files,
    dag=dag,
)

for j in range(0, len(download_tasks)):
    start >> download_tasks[j] >> decompress_tasks[j] >> wait

wait >> delete_staging_files >> end
