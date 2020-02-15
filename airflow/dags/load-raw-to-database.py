from airflow.models import DAG
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from datetime import timedelta, datetime
import ast
from pprint import pprint
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
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='load-raw-to-database', default_args=args, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)


fmt = "%Y-%m-%d"
sdate = datetime.strptime(date_range['date_start'], fmt)
edate = datetime.strptime(date_range['date_end'], fmt)

delta = edate - sdate

for t in config['etl_tasks']:

    spark_load_to_pg = []
    load_to_raw = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        base_date = day.replace(day=1).strftime("%Y-%m-%d")

        folder = config['etl_tasks'][t]['folder']
        file = config['etl_tasks'][t]['file'].replace(".xz", "")

        filepath = 'data/raw/{}/{}/{}_{}'.format(base_date, folder, download_file_day, file)


        load_to_pg = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                        '/spark-urbs-processing/jars/postgresql-42.2.8.jar  /spark-urbs-processing/load_to_postgresql.py -f {} -t {}' \
            .format(filepath, folder)

        spark_load_to_pg.append(DockerOperator(
            task_id='spark_etl_to_pg_{}_{}'.format(download_file_day, file),
            image='altr/spark',
            api_version='auto',
            auto_remove=True,
            environment={
                'PYSPARK_PYTHON': "python3",
                'SPARK_HOME': "/spark"
            },
            volumes=['/mestrado/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing'
                , '/mestrado/master-thesis/airflow/data:/data'],
            command=load_to_pg,
            docker_url='unix://var/run/docker.sock',
            network_mode='host', dag=dag
        ))


        query = """INSERT INTO {table} SELECT * FROM {table}_stg""".format(table = folder)

        load_to_raw.append(PostgresOperator(postgres_conn_id='airflow_dw', task_id='load_data_{}_{}'.format(folder,download_file_day), sql=query, dag=dag))


    start >> spark_load_to_pg[0] >> load_to_raw[0]
    for j in range(0, len(spark_load_to_pg)-1):
        spark_load_to_pg[j] >> load_to_raw[j] >> spark_load_to_pg[j+1] >> load_to_raw[j+1]
    spark_load_to_pg[len(spark_load_to_pg)-1] >> load_to_raw[len(spark_load_to_pg)-1] >> end