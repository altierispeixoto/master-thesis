import logging
from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase
from airflow.operators.dummy_operator import DummyOperator

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


def load_into_neo4j():

    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        cypher_query = """
        LOAD CSV WITH HEADERS FROM "file:///line.csv" AS row
        CREATE (l:Line)
        set   l.line_code  = row.cod
         ,l.category   = row.categoria
         ,l.name       = row.nome
         ,l.color      = row.color
         ,l.card_only  = row.somente_cartao
        RETURN l
        """
        result = session.run(cypher_query)
        logging.info("Execution of backfill_blocks_into_neo4j: %s", result.summary().counters)



"""Build DAG."""
dag = DAG('import-neo4j', default_args=DEFAULT_ARGS, schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)

load_into_neo4j_task = PythonOperator(
        task_id="load_into_neo4j",
        python_callable=load_into_neo4j,
        dag=dag
    )

start >> load_into_neo4j_task

#https://github.com/blockchain-etl/bitcoin-etl-airflow-neo4j/blob/master/dags/dag_btc_to_neo4j.py