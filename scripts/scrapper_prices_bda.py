from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np
import time
import requests
import re
from datetime import datetime
import os



def scrap_prices():
    #Lista de URLs a scrappear
    urls = [
        'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-leche-entera-3-tenor-graso-lechelita-sch-1-ltr/_/A-00518044-00518044-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-yogur-bebible-entero-vainilla-lechelita-sch-900-grm/_/A-00527826-00527826-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-manteca-tonadita-baja-en-lactosa-200-gr/_/A-00481466-00481466-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-manteca-tonadita-baja-en-lactosa-200-gr/_/A-00481466-00481466-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-manteca-multivitaminas-la-serenisima-200gr/_/A-00495534-00495534-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-rollo-de-cocina-campanita-180-panos-paquete-3-unidades/_/A-00224749-00224749-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-yerba-mate-suave-playadito-500-gr/_/A-00502007-00502007-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-azucar-superior-real-ledesma-paq-1-kgm/_/A-00218834-00218834-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-variedad-surtidas-terrabusi-paq-170-grm/_/A-00532286-00532286-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-galletitas-crackers-clasicas-express-paq-101-grm/_/A-00521346-00521346-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-pan-blanco-fargo-rodajas-finas-bsa-560-grm/_/A-00268428-00268428-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-pan-de-mesa-blanco-lactal-bsa-500-grm/_/A-00530650-00530650-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-pan-blanco--bimbo-bsa-550-grm/_/A-00495379-00495379-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-jamon-cocido-feteado-primera-marc-xkg/_/A-00035168-00035168-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-queso-de-maquina--xkg-1-kgm/_/A-00040612-00040612-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-muzzarella--dona-aurora-xkg/_/A-00019971-00019971-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-huevo-blanco-grand--cja-12-uni/_/A-00022865-00022865-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-hamburguesas-swift--12-uni-x-80-gr-clasicas/_/A-00290730-00290730-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-tirabuzon-lucchetti-------paquete-500-gr/_/A-00461486-00461486-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-pure-de-tomate-salsati-tetrabrik-520-gr/_/A-00169714-00169714-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-pulpa-de-tomate-salsati-tetrabrik-520-gr/_/A-00169716-00169716-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-tapa-p-pascualina-sin-tacc-la-saltena-fwp-380-grm/_/A-00470274-00470274-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-harina-trigo-000-morixe-paq-1-kgm/_/A-00480051-00480051-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-agua-mineralizada-artificialmente-con-gas-bajo-en-sodio-cellier-2-l/_/A-00479242-00479242-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-galletitas--criollitas-paq-300-grm/_/A-00099168-00099168-200'
        ,'https://www.cotodigital3.com.ar/sitios/cdigi/producto/-galletitas-crackers-clasicas-media-tarde-pack-familiar-x3-unidades-paq-315-grm/_/A-00532069-00532069-200'
        
       ]

    #Definiciones para la corrida diaria
    fecha = datetime.today().strftime('%Y-%m-%d')
    fecha_file = datetime.today().strftime('%Y%m%d')
    fecha_folder = datetime.today().strftime('%Y%m')

    #Levantamos el ultimo archivo actualizado al dia anterior
    df_historia = pd.read_csv("/home/bjuanm/airflow/dags/files/precios_coto_bda.csv")

    for url in set(urls):
        try:
            html_content = requests.get(url).text #request con el html de la url

            soup = BeautifulSoup(html_content, "html.parser") #convertir texto a estructura html
            price_html = soup.find_all('span', class_="atg_store_productPrice") #seccion con el precio de ese dia
            price_regular_html = soup.find_all('span', class_="price_regular_precio") #seccion con el precio regular (en caso de oferta)
            name_html = soup.find('h1', {'class': 'product_page'}) #nombre del producto

            product_price = float(str(price_html).split('$')[1].split('\n')[0].replace(',','.')) #limpiamos el precio
            product_name = name_html.text.replace('\n','').replace('\t','').replace('\r','') #limpiamos el nombre

            if price_regular_html == []: #si hay oferta, usamos el precio regular, si no, el precio del dia es el precio regular
                price_regular = product_price
            else: 
                price_regular = float(price_regular_html[0].text.split('$')[1])

            to_append = [fecha, product_name, product_price, price_regular] #appendeamos la fila al dataframe historico
            row = pd.Series(to_append, index = df_historia.columns)
            df_historia = df_historia.append(row, ignore_index = True)

        except:
            pass


    path = f"/home/bjuanm/airflow/dags/files/{fecha_folder}_bda" #path para crear uan carpeta por cada mes 
    path_exists = os.path.exists(path)

    if not path_exists: #guardamos los archivos
        os.makedirs(path)
    df_historia.to_csv(f"/home/bjuanm/airflow/dags/files/{fecha_folder}_bda/precios_coto_bda_{fecha_file}.csv", index = False)
    df_historia.to_csv("/home/bjuanm/airflow/dags/files/precios_coto_bda.csv", index = False)

    