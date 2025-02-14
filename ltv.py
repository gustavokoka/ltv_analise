
import pandas as pd
import plotly.express as px
from statsmodels.tsa.seasonal import seasonal_decompose
import logging
import datetime
import time

import dash
from dash import dcc, html
from dash.dependencies import Input, Output

import calendar
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from ipywidgets import interact, widgets
from matplotlib.dates import MonthLocator, DateFormatter
import geopandas as gpd
from shapely.geometry import Point
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import geopandas as gpd
from mpl_toolkits.axes_grid1 import make_axes_locatable
#from fbprophet import Prophet
from sklearn.metrics import mean_squared_error
import numpy as np
import plotly.graph_objects as go
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month
from sklearn.metrics.pairwise import euclidean_distances
from pyspark.sql.functions import year, month, sum as sum_
from scipy.spatial.distance import euclidean
from prophet import Prophet
from pyspark.sql.functions import col, countDistinct , desc , count , length
from pyspark.sql.functions import split, col, when
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, dayofmonth, last_day ,to_date, lit, isnull
from pyspark.sql.types import IntegerType
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import date_format

import schedule
from pyspark.sql.functions import col, to_timestamp, date_format

import pyspark
import pandas as pd
import boto3
import sagemaker
import sagemaker.feature_store.feature_store as fs
import databricks.connect
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, mean_absolute_error
from sklearn.metrics import mean_squared_error, root_mean_squared_error

import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
import plotly.graph_objects as go
from itertools import product
import random
import statsmodels.api as sm
import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, months_between, current_date,expr

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from databricks.connect import DatabricksSession
from datetime import datetime

# Get spark
spark = databricks.connect.DatabricksSession.builder.getOrCreate()







def analisar_permanencia(tabela_sdf):
    """
    Analisa o tempo de permanência dos contratos e gera um gráfico interativo.

    Parâmetros:
    tabela_sdf (Spark DataFrame): DataFrame do Spark contendo 'data_inicio' e 'anos_contrato'.

    Retorno:
    fig (plotly.graph_objects.Figure): Gráfico interativo com análise do tempo de permanência.
    """
    # Converter para Pandas
    df_permanencia = tabela_sdf.select("data_inicio", "anos_contrato").toPandas()

    # Converter start_date para datetime
    df_permanencia["data_inicio"] = pd.to_datetime(df_permanencia["data_inicio"])

    # Criar coluna com o ano do contrato
    df_permanencia["ano_inicio"] = df_permanencia["data_inicio"].dt.year

    # Obter o ano atual
    ano_atual = datetime.today().year

    # Calcular estatísticas por ano
    estatisticas_por_ano = df_permanencia.groupby("ano_inicio")["anos_contrato"].agg(["median", "mean"]).reset_index()

    # Calcular o tempo máximo possível para cada ano (tempo entre ano de início e hoje)
    estatisticas_por_ano["tempo_maximo_possivel"] = ano_atual - estatisticas_por_ano["ano_inicio"]

    # Calcular a diferença entre o tempo máximo e a mediana
    estatisticas_por_ano["diferenca"] = estatisticas_por_ano["tempo_maximo_possivel"] - estatisticas_por_ano["median"]

    # Criar gráfico interativo
    fig = go.Figure()

    # Linha da mediana do tempo de permanência por ano
    fig.add_trace(go.Scatter(
        x=estatisticas_por_ano["ano_inicio"],
        y=estatisticas_por_ano["median"],
        mode="lines+markers",
        name="Mediana do Tempo de Permanência",
        line=dict(color="blue")
    ))

    # Linha da média do tempo de permanência por ano
    fig.add_trace(go.Scatter(
        x=estatisticas_por_ano["ano_inicio"],
        y=estatisticas_por_ano["mean"],
        mode="lines+markers",
        name="Média do Tempo de Permanência",
        line=dict(color="green")
    ))

    # Linha do tempo máximo possível
    fig.add_trace(go.Scatter(
        x=estatisticas_por_ano["ano_inicio"],
        y=estatisticas_por_ano["tempo_maximo_possivel"],
        mode="lines",
        name="Tempo Máximo Possível",
        line=dict(color="red", dash="dash")
    ))

    # Layout do gráfico
    fig.update_layout(
        title="Detecção do Viés no Tempo de Permanência",
        xaxis_title="Ano de Início do Cliente",
        yaxis_title="Tempo de Permanência (anos)",
        showlegend=True
    )

    return fig


