# %%
import os
import sys

scriptpath = "/home/altieris/master-thesis/source/fromnotebooks/src/sparketl.py"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.getcwd())

from src.sparketl import ETLSpark
# %% markdown
# ##### **LINHAS**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/linhas'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/linhas/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **PONTOS LINHA**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/pontoslinha/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/pontoslinha/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **POIS**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/pois/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/pois/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **SHAPELINHA**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/shapelinha/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/shapelinha/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **TABELALINHA**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/tabelalinha/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/tabelalinha/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **TABELAVEICULO**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/tabelaveiculo/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/tabelaveiculo/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **TRECHOSITINERARIOS**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/trechositinerarios/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/trechositinerarios/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **VEICULOS**
# %%
source_path = '/home/altieris/datascience/data/curitibaurbs/raw/veiculos/'
target_path = '/home/altieris/datascience/data/curitibaurbs/processed/veiculos/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path,coalesce=5)
print('vehicles data has been processed!')
# %%
