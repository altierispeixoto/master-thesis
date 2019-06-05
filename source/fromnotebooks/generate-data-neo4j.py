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

processed_path = '/home/altieris/datascience/data/urbs/processed/'

-------------------------------------------------------------------------------
linhas = sqlContext.read.parquet(processed_path+'linhas/')
linhas.registerTempTable("linhas")

target_path = '/home/altieris/datascience/data/urbs/processed/lines-neo4j/'

query = """
select cod,categoria_servico,nome,nome_cor,somente_cartao from linhas
"""

sqlContext.sql(query).coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)


-------------------------------------------------------------------------------

pontosLinha = sqlContext.read.parquet(processed_path+'pontoslinha/')
pontosLinha.registerTempTable("pontos_linha")


query = """
select distinct
       nome
      ,num
      ,tipo
      ,lat
      ,lon
      from pontos_linha
"""

target_path = '/home/altieris/datascience/data/urbs/processed/busstops-neo4j/'

sqlContext.sql(query).coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)


-------------------------------------------------------------------------------

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

sqlContext.sql(query_view_rota_sequenciada)

# where year ='2019' and month='03' and day='14'
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
         where cod_linha = '507'
"""

target_path = '/home/altieris/datascience/data/urbs/processed/routes-neo4j/'

sqlContext.sql(query_rota_sequenciada).coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)

-------------------------------------------------------------------------------
query = """
with start_end as (
    select cod,sentido,min(int(seq)) as start_trip, max(int(seq)) as end_trip
         from pontos_linha
         where COD = '507' -- and sentido = 'Horario'
    group by cod,sentido
)
 select ps.cod as line_code
       ,ps.sentido
       ,ps.num as origin
       ,ps.nome as ponto_origem
       ,pe.num as destination
       ,pe.nome as ponto_destino
  from start_end  ss
     inner join pontos_linha ps on (ps.cod = ss.cod  and ps.sentido = ss.sentido and ps.seq = ss.start_trip)
     inner join pontos_linha pe on (pe.cod = ss.cod  and pe.sentido = ss.sentido and pe.seq = ss.end_trip)

"""

target_path = '/home/altieris/datascience/data/urbs/processed/trip-endpoints-neo4j/'

sqlContext.sql(query).coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)


-------------------------------------------------------------------------------

tabelaLinha = sqlContext.read.parquet(processed_path+'tabelaveiculo/')
tabelaLinha.registerTempTable("tabela_veiculo")
query = """
select cod_linha
      ,cod_ponto
      ,horario
      ,nome_linha
      ,tabela
      ,veiculo
      from tabela_veiculo
      where cod_linha = '507' and veiculo ='EL309'
      order by cod_linha,horario
"""
target_path = '/home/altieris/datascience/data/urbs/processed/schedules-neo4j/'

sqlContext.sql(query).coalesce(1) \
    .write.mode('overwrite')      \
    .option("header", "true")     \
    .format("csv")                \
    .save(target_path)
