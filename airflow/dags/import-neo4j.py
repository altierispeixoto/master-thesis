import logging
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import yaml

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
NEO4J_URI = 'bolt://10.5.0.9:7687' #Variable.get('NEO4J_URI')
NEO4J_USER = "neo4j" #Variable.get('NEO4J_USER')
NEO4J_PASSWORD = "h4ck3r" #Variable.get('NEO4J_PASSWORD')

config = yaml.load(open('./dags/config/data.yml'), Loader=yaml.FullLoader)


def load_into_neo4j(ds, cypher_query, **kwargs):

    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        result = session.run(cypher_query)
        logging.info("Execution: %s", result.summary().counters)



"""Build DAG."""
dag = DAG('import-neo4j', default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False)
start = DummyOperator(task_id='start', dag=dag)
dummy = DummyOperator(task_id='wait', dag=dag)
spark_load_from_pg = []
rename_files = []
load_into_neo4j_tasks = []

for t in config['etl_queries']:

    load_from_pg = '/spark/bin/spark-submit --master local[*] --driver-class-path ' \
                   '/simple-app/jars/postgresql-42.2.8.jar  /simple-app/load_from_postgresql.py -q "{}" -f {}' \
        .format(config['etl_queries'][t], t)

    spark_load_from_pg.append(DockerOperator(
        task_id='spark_etl_from_pg_{}'.format(t),
        image='altr/spark',
        api_version='auto',
        auto_remove=True,
        environment={
            'PYSPARK_PYTHON': "python3",
            'SPARK_HOME': "/spark"
        },
        volumes=['/home/altieris/master-thesis/airflow/simple-app:/simple-app'
            , '/home/altieris/master-thesis/airflow/data:/data'],
        command=load_from_pg,
        docker_url='unix://var/run/docker.sock',
        network_mode='host', dag=dag
    ))

    rename_files.append(BashOperator(
        task_id='move_file_{}'.format(t),
        bash_command="cp /usr/local/airflow/data/processed/{file}/*.csv /usr/local/airflow/neo4j/import/{file}.csv".format(
            file=t),
        dag=dag,
    ))

for neo in config['neo4j_import']:
    load_into_neo4j_tasks.append(PythonOperator(
        task_id="load_into_neo4j_{}".format(neo),
        provide_context=True,
        op_kwargs=config['neo4j_import'][neo],
        python_callable=load_into_neo4j,
        dag=dag
    ))

for j in range(0, len(spark_load_from_pg)):
    start >> spark_load_from_pg[j] >> rename_files[j] >> dummy

for i in range(0, len(load_into_neo4j_tasks)):
    dummy >> load_into_neo4j_tasks[i]

# start >> load_into_neo4j_task

#https://github.com/blockchain-etl/bitcoin-etl-airflow-neo4j/blob/master/dags/dag_btc_to_neo4j.py