# %% markdown
# ### **EXPLORATORY DATA ANALISYS**
# %% markdown
# #### **ALTIERIS M. PEIXOTO**
# %% markdown
# A partir da definição do tema do trabalho, desenvolva uma análise exploratória inicial sobre os dados.
# As análises podem seguir as estratégias disponibilizadas pelos professores no Moodle, mas precisam apresentar os insights encontrados para os dados do seu problema.
#
# Elabore um documento descrevendo sua análise exploratória usando elementos visuais adequados.
#
# **O documento deve conter:**
#
# 1. Descrição das fontes de dados
# 2. Descrição das principais variáveis (features)
# 3. Análises iniciais de distribuição e/ou correlação com gráficos adequados
# 4. Conclusões/ideias/hipóteses iniciais

# %% markdown
# #### **1. Descrição das fontes de dados**
# %% markdown
# No contexto de Cidades Inteligentes, um dos tópicos emergententes é o transporte público e como este influencia diretamente no dia a dia da população.
# Através dos dados disponibilizados pela URBS - Urbanização de Curitiba através do site http://dadosabertos.c3sl.ufpr.br/curitibaurbs/ espera-se ter uma melhor idéia da qualidade do transporte público na região de Curitiba.
#
# O presente trabalho tem como proposta realizar uma breve análise exploratória dos dados disponíveis no site, assim como gerar insights para a evolução do trabalho final da displina.
# %% markdown
# #### **2. Descrição das principais variáveis (features)**
# %% markdown
# Os dados são disponibilizados diáriamente no portal de dados abertos, entretanto são sempre referentes ao dia anterior e em formato JSON.
#
# Foi realizada uma coleta dos dados de 01/01/2019 à 31/03/2019 e foi utilizado o [Apache Spark](https://spark.apache.org/) para o processamento das informações.
#
# **OBS:** Foi realizado uma filtragem nos dados para análise do dia 14-03-2019
# %% markdown
# #### **INIT SPARK CONTEXT AND SET CONFIGURATIONS**
# %%
import findspark
findspark.init('/usr/local/spark')
# %%
import pyspark
import random
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '4G')
        .set('spark.driver.maxResultSize', '3G'))

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)
# %% markdown
# #### **LOAD DATA FILES**
# %%
processed_path = '/home/altieris/datascience/data/urbs/processed/'
# %%
linhas = sqlContext.read.parquet(processed_path+'linhas/')
linhas.registerTempTable("linhas")
# %%
pontosLinha = sqlContext.read.parquet(processed_path+'pontoslinha/')
pontosLinha.registerTempTable("pontos_linha")
# %%
tabelaVeiculo = sqlContext.read.parquet(processed_path+'tabelaveiculo/')
tabelaVeiculo.registerTempTable("tabela_veiculo")
# %%
tabelaLinha = sqlContext.read.parquet(processed_path+'tabelalinha/')
tabelaLinha.registerTempTable("tabela_linha")
# %%
trechosItinerarios = sqlContext.read.parquet(processed_path+'trechositinerarios/')
trechosItinerarios.registerTempTable("trechos_itinerarios")
# %%
categoriasOnibus = trechosItinerarios.select('COD_CATEGORIA','NOME_CATEGORIA').distinct()
categoriasOnibus.registerTempTable("categorias_onibus")
# %%
tipoItinerarios = trechosItinerarios.select('COD_ITINERARIO','NOME_ITINERARIO').distinct()
tipoItinerarios.registerTempTable("tipos_itinerarios")
# %%
empresasOnibus = trechosItinerarios.select("COD_EMPRESA","NOME_EMPRESA").distinct()
empresasOnibus.registerTempTable("empresas_onibus")
# %%
position_events = sqlContext.read.parquet(processed_path+'veiculos/')
position_events.registerTempTable("veiculos")
# %% markdown
# #### **SHOW DATA**
# %%
def executeQuery(table_name):
    query = 'select * from {} limit 10'.format(table_name)

    return sqlContext.sql(query)
# %% markdown
# #### **Linhas de ônibus**
# %% markdown
# Todas as linhas da Rede Integrada do Transporte Coletivo de Curitiba.
# %%
executeQuery('linhas').toPandas().head(10)
# %% markdown
# #### **Pontos de ônibus**
# %% markdown
# Todos os pontos da linha.
# %%
executeQuery('pontos_linha').toPandas().head(10)
# %% markdown
# #### **Tabela de horários de cada ônibus**
# %% markdown
# O número da tabela horária que o veículo executou.
# %%
executeQuery('tabela_veiculo').toPandas().head(10)
# %% markdown
# #### **Tabela horária de cada linha**
# %% markdown
# Tabela horária de cada linha.
# %%
executeQuery('tabela_linha').toPandas().head()
# %% markdown
# #### **Tabela de trechos dos itinerários de cada linha**
# %% markdown
# Trechos dos itinerários das linhas.
# %%
executeQuery('trechos_itinerarios').drop('CODIGO_URBS','NOME_EMPRESA','NOME_CATEGORIA','NOME_LINHA','NOME_ITINERARIO','STOP_NAME','TIPO_TRECHO').toPandas().head()
# %% markdown
# #### **Categorias de ônibus**
# %%
executeQuery('categorias_onibus').toPandas().head()
# %% markdown
# #### **Empresas de transporte público**
# %%
executeQuery('empresas_onibus').toPandas().head(15)

# %% markdown
# #### **Dados de rastreamento dos ônibus**
# %%
executeQuery('veiculos').toPandas().head()

# %% markdown
# #### **3. Análises inciais de distribuição e/ou correlação com gráficos adequados**
# %%
%matplotlib inline

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="ticks", color_codes=True)

sns.set(rc={'figure.figsize':(25.7,8.27)})

# %% markdown
# ##### **Nº de linhas que cada empresa de ônibus opera**
# %%
query = "select count(distinct ti.cod_linha) as nr_linhas,emp.nome_empresa " \
        "from trechos_itinerarios ti inner join empresas_onibus emp on (ti.cod_empresa = emp.cod_empresa) " \
        "where year = '2019' and month = '03' and day='14' " \
        "group by emp.nome_empresa "\
        "order by nr_linhas desc"

linhas_empresa = sqlContext.sql(query).toPandas()
# %%
plt.figure(figsize=(25.7,8.27))
sns.barplot(x="nome_empresa", y="nr_linhas", data=linhas_empresa)
# %%
query = "select  cat.nome_categoria,emp.nome_empresa,count( distinct li.cod) as qtd_linhas " \
"from trechos_itinerarios ti " \
"inner join categorias_onibus cat on (ti.cod_categoria = cat.cod_categoria) " \
"inner join empresas_onibus emp  on (ti.cod_empresa = emp.cod_empresa) " \
"inner join linhas li on (ti.cod_linha = li.cod and ti.year = li.year  and ti.month = li.month and ti.day = li.day) " \
"where ti.year = '2019' and ti.month = '03' and ti.day = '14' group by cat.nome_categoria,emp.nome_empresa order by qtd_linhas desc"

nr_onibus_categoria_empresa = sqlContext.sql(query).toPandas()
# %%
 (fig, ax) = plt.subplots(2,5,figsize=(25,20))

ax[0][0].set(ylim=(0, 40))
ax[0][1].set(ylim=(0, 40))
ax[0][2].set(ylim=(0, 40))
ax[0][3].set(ylim=(0, 40))
ax[0][4].set(ylim=(0, 40))

ax[1][0].set(ylim=(0, 40))
ax[1][1].set(ylim=(0, 40))
ax[1][2].set(ylim=(0, 40))
ax[1][3].set(ylim=(0, 40))
ax[1][4].set(ylim=(0, 40))


sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='GLORIA/PONTUAL')],ax=ax[0][0])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='REDENTOR/TRANSBUS')],ax=ax[0][1])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='SORRISO/PIONEIRO')],ax=ax[0][2])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='CCD/PIONEIRO')],ax=ax[0][3])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='EXPRESSO AZUL/TRANSBUS')],ax=ax[0][4])

sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='S?O JOSE/PIONEIRO')],ax=ax[1][0])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='MERCES/PONTUAL')],ax=ax[1][1])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='ARAUCARIA/TRANSBUS')],ax=ax[1][2])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='SANTO ANTONIO/PONTUAL')],ax=ax[1][3])
sns.barplot(x="nome_empresa", y="qtd_linhas", hue="nome_categoria", data=nr_onibus_categoria_empresa[(nr_onibus_categoria_empresa['nome_empresa']=='TAMANDARE/PIONEIRO')],ax=ax[1][4])

# %% markdown
# ##### **Linhas com o maior número de pontos de ônibus**
# %%
query = "select  pont.cod,li.nome ,count(distinct pont.num) as qtd_pontos " \
"from pontos_linha pont inner join linhas li on (pont.cod = li.cod and pont.year=li.year and pont.month=li.month and pont.day=li.day) " \
"where li.year = '2019' and li.month='03' and li.day='14' group by pont.cod,li.nome order by qtd_pontos desc"

pontos_linha = sqlContext.sql(query).toPandas()
# %%
points = pontos_linha.head(20)
points.plot("nome", "qtd_pontos", kind="barh" )
# %%
points.head(20)
# %% markdown
# ##### **Breve análise de dados de rastreamento dos ônibus**
# %%
query = "select  veic.cod_linha,li.nome, veic.veic,count(distinct veic.dthr) as nr_posicoes " \
"from veiculos veic inner join linhas li on (veic.cod_linha = li.cod) " \
"where TO_DATE(CAST(UNIX_TIMESTAMP(veic.dthr, 'dd/MM/yyyy') AS TIMESTAMP)) = '2019-03-14' group by veic.cod_linha,li.nome,veic.veic "

result = sqlContext.sql(query).toPandas()
# %%
result.head(10)
# %% markdown
# ##### **Top 10 ônibus com o maior número de posicionamento e suas respectivas linhas de operação.**
# %%
result.sort_values(by=['nr_posicoes'], ascending=False).head(10)
# %% markdown
# ##### **Top 10 ônibus com o menor número de posicionamento e suas respectivas linhas de operação.**
# %%
result.sort_values(by=['nr_posicoes'], ascending=True).head(10)
# %% markdown
# #### **Análise de frequência de posicionamento do veículo com o menor número de posições.**
# %%
query = "select  distinct veic.cod_linha,li.nome, veic.veic, veic.lat,veic.lon, veic.dthr " \
"from veiculos veic inner join linhas li on (veic.cod_linha = li.cod) " \
"where TO_DATE(CAST(UNIX_TIMESTAMP(veic.dthr, 'dd/MM/yyyy') AS TIMESTAMP)) = '2019-03-14' and veic.cod_linha ='809' and veic.veic = 'LA851' order by veic.dthr desc"

result = sqlContext.sql(query).toPandas()
# %%
result.head()
# %% markdown
# Aparentemente o veículo está apresentando uma frequência de posicionamento menor que 10 seguntos.
# Suficiente para análises posteriores.
# %% markdown
# #### **Análise de frequência de posicionamento do veículo com o maior número de posições.**
# %%
query = "select  distinct veic.cod_linha,li.nome, veic.veic,veic.lat,veic.lon,veic.dthr " \
"from veiculos veic inner join linhas li on (veic.cod_linha = li.cod) " \
"where TO_DATE(CAST(UNIX_TIMESTAMP(veic.dthr, 'dd/MM/yyyy') AS TIMESTAMP)) = '2019-03-14' and veic.cod_linha ='924' and veic.veic = 'MA013' order by veic.dthr desc"

result = sqlContext.sql(query).toPandas()
# %%
result.head()
# %% markdown
# Aparentemente o veículo também está apresentando uma frequência de posicionamento menor que 10 seguntos.
# Suficiente para análises posteriores.
# %% markdown
# #### **4. Conclusões/ideias/hipóteses iniciais**
# %% markdown
# Através desta breve análise exploratória dos dados fonecidos pela URBS pode-se analisar os dados de maior importância, assim como a qualidade dos mesmos.
# Como passos futuros será iniciado a criação da base de dados para análise de link stream para detecção de padrões de bus bunching utilizando o Neo4J.
# %%
