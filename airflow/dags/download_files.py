from airflow.models import DAG
import airflow
from pprint import pprint
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import wget
from datetime import date, timedelta, datetime
import ast
import urllib3
import glob


def download_files(ds, **kwargs):

    pprint("Date range: {}".format(date_range))
    pprint("Base URL: {}".format(base_url))
    pprint("Files: {}".format(files))

    fmt = "%Y-%m-%d"
    sdate = datetime.strptime(date_range['date_start'], fmt)
    edate = datetime.strptime(date_range['date_end'], fmt)

    delta = edate - sdate
    for file_name in files:
        pprint("Filename: {}".format(file_name))

        for i in range(delta.days + 1):
            day = sdate + timedelta(days=i)
            download_file_day = day.strftime("%Y_%m_%d")
            url = '{}{}_{}'.format(base_url, download_file_day, file_name)
            pprint("Downloading: {}".format(url))

            file = 'data/staging/{}_{}'.format(download_file_day, file_name)

            http = urllib3.PoolManager()
            r = http.request('GET', url, preload_content=False)

            with open(file, 'wb') as out:
                while True:
                    data = r.read()
                    if not data:
                        break
                    out.write(data)

            r.release_conn()

    return "download realizado com sucesso"



import lzma

def decompress_files(ds, **kwargs):

    files = glob.glob("data/staging/*.xz")
    for file in files:
        binary_data_buffer = lzma.open(file, mode='rt', encoding='utf-8').read()
        f = file.replace('.xz', '').replace('staging', 'raw')
        with open(f, 'w') as a:
            a.write(binary_data_buffer)


date_range = ast.literal_eval(Variable.get("date_range"))
files = ast.literal_eval(Variable.get("file_name"))
base_url = Variable.get("base_url")

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


dag = DAG(dag_id='download_files', default_args=args, schedule_interval=None)


start = DummyOperator(task_id='start', dag=dag)

download = PythonOperator(
    task_id='download_files',
    provide_context=True,
    python_callable=download_files,
    dag=dag,
)

decompress = PythonOperator(
    task_id='decompress_files',
    provide_context=True,
    python_callable=decompress_files,
    dag=dag,
)


start >> download >> decompress