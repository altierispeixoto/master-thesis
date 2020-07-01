import findspark

findspark.init()
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as functions
import gc

from src.neo4jcrud import UrbsNeo4JDatabase


def insert_events(row):
    NEO4J_URI = 'bolt://172.16.1.118:7687'
    NEO4J_USER = 'neo4j'
    NEO4J_PASSWORD = 'neo4j2018'

    conn = UrbsNeo4JDatabase(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    conn.create_position(row[0], row[1], row[2], row[3], row[4],row[5])
    conn.close()

class DataLoader:

    def __init__(self):
        self.conf = SparkConf().setAppName("App")
        self.conf = (self.conf.setMaster('local[*]')
                     .set('spark.executor.memory', '5G')
                     .set('spark.driver.memory', '10G')
                     .set('spark.driver.maxResultSize', '10G'))

        self.sc = SparkContext.getOrCreate(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)

    def load_data(self, src):
        return self.sqlContext.read.parquet(src)

    def create_empresas_onibus(self, trechosItinerarios, conn):
        empresas_df = trechosItinerarios.select("COD_EMPRESA", "NOME_EMPRESA").distinct().toPandas()
        [conn.create_bus_company(row['COD_EMPRESA'], row['NOME_EMPRESA']) for index, row in empresas_df.iterrows()]

    #### CATEGORIAS ONIBUS
    def create_categorias_onibus(self, trechosItinerarios, conn):
        categoriasOnibus = trechosItinerarios.select('COD_CATEGORIA', 'NOME_CATEGORIA').distinct()

        categorias_df = categoriasOnibus.toPandas()
        [conn.create_bus_category(row['COD_CATEGORIA'], row['NOME_CATEGORIA']) for index, row in
         categorias_df.iterrows()]

    def create_bus_stops(self, pontos_linha, conn):
        pontos_df = pontos_linha.select(['nome', 'num', 'tipo', 'lat', 'lon']).filter(
            "year ='2019' and month='03' and day = '14'").distinct().toPandas()

        [conn.create_bus_stop(row['nome'], row['num'], row['tipo'], row['lat'], row['lon']) for index, row in
         pontos_df.iterrows()]

    def create_routes(self, conn):
        linhas = self.sqlContext.read.parquet('/home/altieris/datascience/data/curitibaurbs/processed/linhas/')
        linhas.registerTempTable("linhas")

        pontosLinha = self.sqlContext.read.parquet(
            '/home/altieris/datascience/data/curitibaurbs/processed/pontoslinha/')
        pontosLinha.registerTempTable("pontos_linha")

        query_view_rota_sequenciada = "CREATE OR REPLACE TEMPORARY VIEW rota_sequenciada AS  " \
                                      "select 	pseq.cod_linha,pseq.sentido_linha,pseq.seq_inicio,pseq.seq_fim,pseq.ponto_inicio,pseq.nome_ponto_inicio " \
                                      ",pseq.ponto_final,pseq.nome_ponto_final,li.CATEGORIA_SERVICO as categoria_servico,li.NOME as nome_linha,li.NOME_COR as nome_cor,li.SOMENTE_CARTAO as somente_cartao " \
                                      ",pseq.year, pseq.month,pseq.day " \
                                      "from (select " \
                                      "p1.COD as cod_linha " \
                                      ",p1.SENTIDO  as sentido_linha " \
                                      ",p1.SEQ      as seq_inicio " \
                                      ",p2.SEQ      as seq_fim " \
                                      ",p1.NUM      as ponto_inicio " \
                                      ",p1.NOME     as nome_ponto_inicio " \
                                      ",p2.NUM      as ponto_final " \
                                      ",p2.NOME     as nome_ponto_final " \
                                      ",p1.year " \
                                      ",p1.month " \
                                      ",p1.day " \
                                      "from pontos_linha P1 " \
                                      "inner join pontos_linha p2 on (p1.SEQ+1 = p2.SEQ and p1.COD = p2.COD and p1.SENTIDO = p2.SENTIDO and p1.year = p2.year and p1.month=p2.month and p1.day=p2.day) " \
                                      ") pseq " \
                                      "inner join linhas       li on (pseq.cod_linha = li.COD and pseq.year = li.year and pseq.month=li.month and pseq.day=li.day) " \
                                      "order by pseq.cod_linha,pseq.sentido_linha,pseq.seq_inicio,pseq.seq_fim "

        self.sqlContext.sql(query_view_rota_sequenciada)

        query_rota_sequenciada = "select cod_linha,sentido_linha,ponto_inicio,nome_ponto_inicio,ponto_final,nome_ponto_final,categoria_servico,nome_linha,nome_cor,somente_cartao " \
                                 "from rota_sequenciada where year ='2019' and month='03' and day='14' "

        rota_sequenciada = self.sqlContext.sql(query_rota_sequenciada)
        rota_sequenciada_df = rota_sequenciada.toPandas()

        [conn.create_bus_lines(row['ponto_inicio'], row['ponto_final'], row['cod_linha'], row['sentido_linha'],
                               row['categoria_servico'], row['nome_linha'], row['nome_cor'],
                               row['somente_cartao']) for index, row in rota_sequenciada_df.iterrows()]

        del rota_sequenciada
        del rota_sequenciada_df
        gc.collect()

#     def create_vehicles(self, vehicles, conn):

#         vehicles_df = vehicles.select('veic').filter("year ='2019' and month='03' and day = '14'  ").distinct().toPandas()
#         del vehicles
#         del vehicles_df
#         gc.collect()

#         [conn.create_bus(row['veic']) for index, row in vehicles_df.iterrows()]



    def create_positions(self, vehicles,vehicle,line_code,date):

        f = "veic = '{}' and cod_linha = '{}' and date='{}'".format(vehicle, line_code,date)

        vehicles.select('veic', 'lat', 'lon','cod_linha', 'event_timestamp','date').filter(f).foreach(insert_events)



        print('Vehicle: {} - Line Code: {}'.format(vehicle,line_code))


    def connect_events(self,vehicle,line_code,date):

        NEO4J_URI = 'bolt://172.16.1.118:7687'
        NEO4J_USER = 'neo4j'
        NEO4J_PASSWORD = 'neo4j2018'

        conn = UrbsNeo4JDatabase(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        conn.connect_events(vehicle,line_code,date)
        conn.close()

    def create_edge_properties(self,vehicle,line_code,date):

        NEO4J_URI = 'bolt://172.16.1.118:7687'
        NEO4J_USER = 'neo4j'
        NEO4J_PASSWORD = 'neo4j2018'

        conn = UrbsNeo4JDatabase(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        conn.create_edge_properties(vehicle,line_code,date)
        conn.close()


#         del vehicles
#         del vehicles_df
#         gc.collect()
