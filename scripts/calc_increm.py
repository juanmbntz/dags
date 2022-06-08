from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np
import time
import requests
import re
from datetime import datetime
import os

def calc_increment():

    #Definiciones para la corrida diaria
    fecha = datetime.today().strftime('%Y-%m-%d')
    fecha_file = datetime.today().strftime('%Y%m%d')
    fecha_folder = datetime.today().strftime('%Y%m')

    #Levantamos el ultimo archivo actualizado al dia anterior
    df_historia = pd.read_csv("/home/bjuanm/airflow/dags/files/precios_coto.csv")   

    df_final = pd.DataFrame(columns = ['fecha','nombre_producto','precio_dia','precio_regular','incremento_diario','incremento_semanal'])

    for producto in df_historia.nombre_producto.unique():
        df_incremento = df_historia[df_historia['nombre_producto'] == producto].sort_values(by='fecha', ascending=True)
        #incrementos = [((df_incremento[i:i+1].precio.values / df_incremento[i-1:i].precio.values)[0]-1)*100 for i in range(1,len(df_incremento))]
        df_incremento['incremento_diario'] = round(df_incremento.precio_regular.pct_change(periods=1)*100,2)
        df_incremento['incremento_semanal'] = round(df_incremento.precio_regular.pct_change(periods=7)*100,2)
        
        df_final = df_final.append(df_incremento, ignore_index = True)



    df_final.to_csv("/home/bjuanm/airflow/dags/files/precios_coto_increm.csv", index = False)