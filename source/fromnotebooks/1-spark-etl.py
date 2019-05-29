# %%
import os
import sys

scriptpath = "/home/altieris/master-thesis/source/fromnotebooks/src/sparketl.py"

# Add the directory containing your module to the Python path (wants absolute paths)
sys.path.append(os.path.abspath(scriptpath))


from src.sparketl import ETLSpark
# %% markdown
# ##### **LINHAS**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/linhas'
target_path = '/home/altieris/datascience/data/urbs/processed/linhas/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **PONTOS LINHA**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/pontoslinha/'
target_path = '/home/altieris/datascience/data/urbs/processed/pontoslinha/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)

# %% markdown
# ##### **TABELALINHA**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/tabelalinha/'
target_path = '/home/altieris/datascience/data/urbs/processed/tabelalinha/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **TABELAVEICULO**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/tabelaveiculo/'
target_path = '/home/altieris/datascience/data/urbs/processed/tabelaveiculo/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)
# %% markdown
# ##### **TRECHOSITINERARIOS**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/trechositinerarios/'
target_path = '/home/altieris/datascience/data/urbs/processed/trechositinerarios/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path)

# %% markdown
# ##### **VEICULOS**
# %%
source_path = '/home/altieris/datascience/data/urbs/raw/veiculos/'
target_path = '/home/altieris/datascience/data/urbs/processed/veiculos/'

etlspark = ETLSpark()
raw_data = etlspark.extract(source_path)
etlspark.transform(raw_data, target_path,coalesce=5)
print('vehicles data has been processed!')
# %%
