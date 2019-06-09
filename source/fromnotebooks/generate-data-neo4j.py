import findspark
findspark.init('/usr/local/spark')

from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

# %%

conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '4G')
        .set('spark.driver.maxResultSize', '3G'))

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)


# -------------------------------------------------------------------------------


def load():
    processed_path = '/home/altieris/datascience/data/urbs/processed/'

    sqlContext.read.parquet(processed_path+'linhas/').registerTempTable("linhas")
    sqlContext.read.parquet(processed_path+'pontoslinha/').registerTempTable("pontos_linha")

    sqlContext.read.parquet(processed_path+'tabelaveiculo/').registerTempTable("tabela_veiculo")

load()

def save(query, target_path):
    sqlContext.sql(query).coalesce(1) \
        .write.mode('overwrite')      \
        .option("header", "true")     \
        .format("csv")                \
        .save(target_path)


def show(query, n=10):
    sqlContext.sql(query).show(n)


def run(query):
    sqlContext.sql(query)


query = """
select cod
      ,categoria_servico
      ,nome
      ,nome_cor
      ,somente_cartao
    from linhas
"""
#show(query)
save(query, target_path='/home/altieris/datascience/data/urbs/processed/lines-neo4j/')

# -------------------------------------------------------------------------------

query = """
select distinct
       nome
      ,num
      ,tipo
      ,lat
      ,lon
      from pontos_linha
"""

save(query, target_path='/home/altieris/datascience/data/urbs/processed/busstops-neo4j/')

# -------------------------------------------------------------------------------

query_view_rota_sequenciada = """
CREATE OR REPLACE TEMPORARY VIEW rota_sequenciada AS
   select 	pseq.cod_linha
           ,pseq.sentido_linha
           ,pseq.seq_inicio
           ,pseq.seq_fim
           ,pseq.ponto_inicio
           ,pseq.nome_ponto_inicio
           ,pseq.ponto_final
           ,pseq.nome_ponto_final
           ,li.CATEGORIA_SERVICO as categoria_servico
           ,li.NOME as nome_linha
           ,li.NOME_COR as nome_cor
           ,li.SOMENTE_CARTAO as somente_cartao
           ,pseq.year
           ,pseq.month
           ,pseq.day
                 from (select
                               p1.COD as cod_linha
                              ,p1.SENTIDO  as sentido_linha
                              ,p1.SEQ      as seq_inicio
                              ,p2.SEQ      as seq_fim
                              ,p1.NUM      as ponto_inicio
                              ,p1.NOME     as nome_ponto_inicio
                              ,p2.NUM      as ponto_final
                              ,p2.NOME     as nome_ponto_final
                              ,p1.year
                              ,p1.month
                              ,p1.day
                              from pontos_linha P1
                              inner join pontos_linha p2
                                  on (p1.SEQ+1 = p2.SEQ
                                       and p1.COD = p2.COD
                                       and p1.SENTIDO = p2.SENTIDO
                                       and p1.year = p2.year
                                       and p1.month=p2.month and p1.day=p2.day)
                              ) pseq
                              inner join linhas li
                                     on (pseq.cod_linha = li.COD
                                     and pseq.year = li.year
                                     and pseq.month=li.month
                                     and pseq.day=li.day
                                     )
                              order by pseq.cod_linha
                                      ,pseq.sentido_linha
                                      ,pseq.seq_inicio
                                      ,pseq.seq_fim
"""

run(query_view_rota_sequenciada)

query_rota_sequenciada = """
    select cod_linha
          ,sentido_linha
          ,ponto_inicio
          ,nome_ponto_inicio
          ,ponto_final
          ,nome_ponto_final
          ,categoria_servico
          ,nome_linha
          ,nome_cor
          ,somente_cartao
         from rota_sequenciada
"""

save(query_rota_sequenciada, target_path='/home/altieris/datascience/data/urbs/processed/routes-neo4j/')

# -------------------------------------------------------------------------------
query = """
with start_end as (
    select  cod
           ,sentido
           ,min(int(seq)) as start_trip
           ,max(int(seq)) as end_trip
         from pontos_linha
    group by cod,sentido
)
 select ps.cod     as line_code
       ,ps.sentido
       ,ps.num     as origin
       ,ps.nome    as ponto_origem
       ,pe.num     as destination
       ,pe.nome    as ponto_destino
  from start_end  ss
     inner join pontos_linha ps on (ps.cod = ss.cod  and ps.sentido = ss.sentido and ps.seq = ss.start_trip)
     inner join pontos_linha pe on (pe.cod = ss.cod  and pe.sentido = ss.sentido and pe.seq = ss.end_trip)

"""

save(query, target_path='/home/altieris/datascience/data/urbs/processed/trip-endpoints-neo4j/')

# -------------------------------------------------------------------------------

query = """
select cod_linha
      ,cod_ponto
      ,horario
      ,nome_linha
      ,tabela
      ,veiculo
      from tabela_veiculo
      order by cod_linha,horario
"""
save(query, target_path='/home/altieris/datascience/data/urbs/processed/schedules-neo4j/')

# --------------------------------------------------------------------------

query = """
with query_1 as (
select cod_linha     as line_code
      ,cod_ponto     as start_point
      ,horario       as start_time
      ,tabela        as time_table
      ,veiculo       as vehicle
      ,lead(horario) over(partition by cod_linha,tabela,veiculo order by cod_linha, horario)   as end_time
      ,lead(cod_ponto) over(partition by cod_linha,tabela,veiculo order by cod_linha, horario) as end_point
      from tabela_veiculo
      order by cod_linha,horario
)
select * from query_1
"""

save(query, target_path='/home/altieris/datascience/data/urbs/processed/trips/')
