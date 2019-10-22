"""This dag ingest bitcoin blockchain into Neo4J"""

import logging
from datetime import datetime, timedelta

import jinja2
from airflow import macros
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from neo4j import GraphDatabase

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
NEO4J_URI = ""#Variable.get('NEO4J_URI')
NEO4J_USER = ""#Variable.get('NEO4J_USER')
NEO4J_PASSWORD = ""#Variable.get('NEO4J_PASSWORD')


def load_into_neo4j(ds):

    # element = kwargs['element']
    # neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    #
    # with neo4j_driver.session() as session:
    #     template_loader = jinja2.FileSystemLoader(searchpath='gcs/dags/cypher/')
    #     template_env = jinja2.Environment(loader=template_loader)
    #     template = template_env.get_template('load-{element}.cypher'.format(element=element))
    #
    #     # Load data files
    #
    #     date_folder = macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d")
    #     prefix = 'neo4j_import/{date_folder}/{element}/'.format(date_folder=date_folder, element=element)
    #     for gs_filename in bucket.list_blobs(prefix=prefix):
    #         uri = 'http://storage.googleapis.com/{bucket}/{gs_filename}'.format(bucket=bucket.name,
    #                                                                             gs_filename=gs_filename.name)
    #         cypher_queries = [query.strip() for query in template.render(uri=uri).split(';') if query]
    #         logging.info(cypher_queries)
    #         for cypher_query in cypher_queries:
    #             result = session.run(cypher_query, timeout=30*60)
    #             logging.info("Execution of load into Neo4J returned: %s", result.summary().counters)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with neo4j_driver.session() as session:
        cypher_query = "MATCH (_b:Block)  WHERE date(_b.timestamp) = date('" + ds + "') " \
                                                                                    "MATCH (b:Block {height: _b.height + 1}) " \
                                                                                    "MERGE (_b)-[:next]->(b)"
        result = session.run(cypher_query)
        logging.info("Execution of backfill_blocks_into_neo4j: %s", result.summary().counters)


def build_dag():

    """Build DAG."""
    dag = DAG('import-neo4j',
              schedule_interval='@daily',
              default_args=DEFAULT_ARGS,
              catchup=True)

    # NOTE: It is import to keep elements of this list in this order since it is required later when loading data

    load_into_neo4j_task = PythonOperator(
        task_id="load_into_neo4j",
        python_callable=load_into_neo4j,
        provide_context=True,
        dag=dag
    )

    return dag


the_dag = build_dag()