from airflow.models import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
import yaml

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)
date_range = ast.literal_eval(Variable.get("date_range"))


args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'pool': 'load-raw-to-processed',
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='load-raw-to-processed', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

delta = edate - sdate

spark_submit = "/spark/bin/spark-submit --master local[*] --executor-memory 4g --driver-memory 4g --conf " \
               "spark.network.timeout=600s "

for t in config['etl_tasks']:

    transform_to_parquet = []
    load_to_raw = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        base_date = day.replace(day=1).strftime("%Y-%m")

        folder = config['etl_tasks'][t]['folder']
        file = config['etl_tasks'][t]['file'].replace(".xz", "")

        filepath = f"data/raw/{base_date}/{folder}/{download_file_day}_{file}"

        load_to_processed = f"{spark_submit} /spark-urbs-processing/load_to_processed.py -f {filepath} -t {folder}"

        transform_to_parquet.append(DockerOperator(
            task_id=f"transform_to_parquet_{download_file_day}_{file}",
            image='altr/spark',
            api_version='auto',
            auto_remove=True,
            environment={
                'PYSPARK_PYTHON': "python3",
                'SPARK_HOME': "/spark"
            },
            volumes=['/work/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing', '/work/datalake:/data'],
            command=load_to_processed,
            docker_url='unix://var/run/docker.sock',
            network_mode='host', dag=dag
        ))

    start >> transform_to_parquet[0]
    for j in range(0, len(transform_to_parquet)-1):
        transform_to_parquet[j] >> transform_to_parquet[j+1]
    transform_to_parquet[len(transform_to_parquet)-1] >> end