from airflow.models import DAG
import logging
import airflow
from pprint import pprint
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from datetime import date, timedelta, datetime
import ast
import urllib3
import glob
import lzma
import yaml
import os

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)


def download_files(ds, folder, file, **kwargs):
    pprint("Date range: {}".format(date_range))
    pprint("Base URL: {}".format(base_url))
    pprint("Filename: {}".format(file))

    fmt = "%Y-%m-%d"
    sdate = datetime.strptime(date_range['date_start'], fmt)
    edate = datetime.strptime(date_range['date_end'], fmt)

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")
        url = '{}{}_{}'.format(base_url, download_file_day, file)
        pprint("Downloading: {}".format(url))

        base_folder = 'data/staging/{}'.format(folder)
        try:
            os.stat(base_folder)
        except:
            os.mkdir(base_folder)

        fd = 'data/staging/{}/{}_{}'.format(folder, download_file_day, file)

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
    files = glob.glob("data/staging/{}/*.xz".format(folder))

    base_folder = 'data/raw/{}'.format(folder)
    try:
        os.stat(base_folder)
    except:
        os.mkdir(base_folder)

    for file in files:
        binary_data_buffer = lzma.open(file, mode='rt', encoding='utf-8').read()
        f = file.replace('.xz', '').replace('staging', 'raw')
        with open(f, 'w') as a:
            a.write(binary_data_buffer)


date_range = ast.literal_eval(Variable.get("date_range"))
base_url = Variable.get("base_url")

args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='download_files', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)

download_tasks = []
decompress_tasks = []

spark_load_to_pg = []

for t in config['etl_tasks']:
    download_tasks.append(PythonOperator(
        task_id='download_{}'.format(t),
        provide_context=True,
        op_kwargs=config['etl_tasks'][t],
        python_callable=download_files,
        dag=dag,
    ))

    decompress_tasks.append(PythonOperator(
        task_id='decompress_{}'.format(t),
        provide_context=True,
        op_kwargs=config['etl_tasks'][t],
        python_callable=decompress_files,
        dag=dag,
    ))

    load_to_pg = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                 '/simple-app/jars/postgresql-42.2.8.jar  /simple-app/load_to_postgresql.py -f {}' \
        .format(config['etl_tasks'][t]['folder'])

    spark_load_to_pg.append(DockerOperator(
        task_id='spark_etl_to_pg_{}'.format(t),
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/simple-app:/simple-app'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=load_to_pg,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    ))


for j in range(0, len(download_tasks)):
    start >> download_tasks[j] >> decompress_tasks[j] >> spark_load_to_pg[j]

# https://itnext.io/how-to-create-a-simple-etl-job-locally-with-pyspark-postgresql-and-docker-ea53cd43311d
