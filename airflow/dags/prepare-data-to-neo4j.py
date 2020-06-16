import airflow
from airflow.models import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
import yaml
import ast

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'pool': 'prepare-data-to-neo4j'
}

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)
date_range = ast.literal_eval(Variable.get("date_range"))

sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

delta = edate - sdate

"""Build DAG."""
dag = DAG('prepare-data-to-neo4j', default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False, max_active_runs=2)
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

spark_load_from_pg = []


def execute_spark_process(task_id, command, dag):
    task = DockerOperator(
        task_id=task_id,
        image='bde2020/spark-master:2.4.4-hadoop2.7',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/work/master-thesis/airflow/spark-urbs-processing:/spark-urbs-processing', '/work/datalake:/data'],
        command=command,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    )

    return task


def process_etl_queries(datareferencia, dag):
    tasks = []
    for t in config['etl_queries']:
        query = config['etl_queries'][t]

        query = query.format(datareferencia=datareferencia)
        load_from_pg = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                       '/spark-urbs-processing/jars/presto-jdbc-0.221.jar  /spark-urbs-processing/load_from_prestodb.py -q "{}" -f {} -d {}' \
            .format(query, t, datareferencia)

        tasks.append(execute_spark_process(f"spark_etl_from_presto_{t}_{datareferencia}", load_from_pg, dag))
    return tasks


for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    datareferencia = day.strftime("%Y-%m-%d")

    # stopevents = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
    #              '/spark-urbs-processing/jars/presto-jdbc-0.221.jar  /spark-urbs-processing/stop-events.py -d {}'.format(
    #     datareferencia)
    #
    tracking_data = '/spark/bin/spark-submit --master local[*] --executor-memory 6g --driver-memory 10g --conf spark.network.timeout=600s --driver-class-path ' \
                    '/spark-urbs-processing/jars/presto-jdbc-0.221.jar  /spark-urbs-processing/tracking-data.py -d {}'.format(
        datareferencia)

    event_stop_edges = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                       '/spark-urbs-processing/jars/presto-jdbc-0.221.jar  /spark-urbs-processing/event-stop-edges.py -d {}'.format(
        datareferencia)

    start >> process_etl_queries(datareferencia, dag) >> end
    # start >> execute_spark_process(f"spark_etl_stop_events-{datareferencia}", stopevents, dag)
    start >> execute_spark_process(f"spark_etl_event_stop_edges-{datareferencia}", event_stop_edges, dag) >> end
    start >> execute_spark_process(f"spark_etl_tracking_data-{datareferencia}", tracking_data, dag) >> end
