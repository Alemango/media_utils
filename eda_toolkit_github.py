#!/usr/bin/env python
# coding: utf-8

# ## eda_toolkit_github
# 
# null

# # Test EDA Toolkit from Git Branch

##RUN##

# In[1]:


!pip install deltalake

##RUN##

# In[2]:


## Load Fabric variable library
variable_library = notebookutils.variableLibrary.getLibrary("variable_library")

## Get GitHub token from Azure key vault
secret_value = notebookutils.credentials.getSecret(variable_library.keyvault_cba_URL, variable_library.repo_cba_token)

## Install GitHub repo with token from Azure key vault
# branch = variable_library.repo_cba_branch
branch = 'feature/eda_toolkit'
get_ipython().system('pip install -U "git+https://{secret_value}@github.com/Vensure-Devops-QA/creai_client-behavior-data-cleaning.git@{branch}"')

##RUN##

# In[3]:

!pip install seaborn

##RUN##

# Import necessary libraries
import json

import pandas as pd

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

from pandas.api.types import is_datetime64_any_dtype

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

##RUN##

# In[4]:


from vensure_client_behavior.processes.eda_toolkit.pipeline import EDAPipeline

##RUN##

# In[5]:


from deltalake import DeltaTable, write_deltalake
table_path = 'abfss://Dev_Vensure_Gold@onelake.dfs.fabric.microsoft.com/lakehouse_gold.Lakehouse/Tables/vensure_dbo' 
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}

##RUN##

# In[6]:


df_cm = DeltaTable(table_path + "/Cases", storage_options=storage_options).to_pandas()
df_cs = DeltaTable(table_path + "/Case_Survey", storage_options=storage_options).to_pandas()

#df_clients_filtered = df_cm[df_cm['isCurrent'] == 1]
#print(f"  Clientes después de filtro: {len(df_clients_filtered)}")

##RUN##

# In[7]:


pipeline = EDAPipeline()

# Cargar DataFrames al pipeline
pipeline.load_dataframe(df_cm, alias='clients')
pipeline.load_dataframe(df_cs, alias='cases')

# Ejecutar pipeline completo
results = pipeline.run_full_pipeline(
    merge_on='uniqueId',
    merge_how='left',
    create_features=True
)

# Acceder a resultados
df_final = results['feature_engineered_dataframe']
quality_report = results['quality_report']

print("\n" + "="*80)
print("RESULTADOS DEL PIPELINE")
print("="*80)

print(f"\nDataFrame final:")
print(f"  Filas: {len(df_final):,}")
print(f"  Columnas: {len(df_final.columns)}")
print(f"  Completeness: {quality_report['completeness_score']:.2f}%")
print(f"  Nulos críticos: {len(quality_report['null_severity']['critical'])}")

print("\nColumnas generadas:")
print(df_final.columns.tolist())

print("\nMuestra de datos:")
print(df_final.head())

print("\nEstadísticas de features agregadas:")
feature_cols = [col for col in df_final.columns if any(x in col for x in ['cases', 'rating', 'aging', 'priority'])]
if feature_cols:
    print(df_final[feature_cols].describe())

##RUN##
