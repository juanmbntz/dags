#dataproc
import pandas as pd
import numpy as np
import pandas.io.sql as psql
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime 
from airflow.hooks.S3_hook import S3Hook

#iteracting with fs
import os
from pathlib import Path

#logging to console
import logging

#global constants
PATH = Path(__file__).parent.parent.resolve()
SQL_FILE_NAME = "query_UBA.sql"


#Logs base setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG,
    )

logger = logging.getLogger(__name__)

def get_data_from_sql():

    #Read SQL file with query to execute
    logger.info(f'Oppening file {PATH}/sql/{SQL_FILE_NAME}')
    sql_file = open(f"{PATH}/sql/{SQL_FILE_NAME}",'r', 
                        encoding='utf-8')\
                        .read()
    
    #Create PostgreSQL hook using postgres connection
    logger.info("Connecting to database...")
    postgres_hook = PostgresHook(postgres_conn_id='conn_universities', schema= 'training')
    pg_cursor = postgres_hook.get_conn().cursor()

    logger.info("Connected succesfully!")
    #execute query
    logger.info("Retrieving data from database")
    pg_cursor.execute(sql_file)
    logger.info("Query executed succesfully!")

    #fetch query data and table structure
    query_data = pg_cursor.fetchall()
    query_columns = [x[0] for x in pg_cursor.description]

    #create files folder if it doesn't exist
    files_folder_path = f'{PATH}/files'
    if not os.path.exists(files_folder_path):
        os.makedirs(files_folder_path)


    #create df and save it into .txt
    pd.DataFrame(   
                data=query_data, 
                columns = query_columns)\
                .to_csv(f'{PATH}/files/UBA_data.txt', index=False)
    
    logger.info(f"File {PATH}/files/UBA_data.txt created succesfully.")
  




def pd_data_processing():

    #read file with zip codes
    #url = 'https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view'
    #zipcode_path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]

    #logger.info(f"Reading zip codes file...")
    #zip_codes_file = pd.read_csv(zipcode_path)\
    #                .rename(columns={'codigo_postal': 'postal_code',
    #                                'localidad': 'location'
    #                                })
    #       
    zip_codes_file = pd.read_csv(f'{PATH}/files/codigos_postales.csv', index_col = False)\
                                        .rename(columns={
                                            'codigo_postal': 'postal_code',
                                            'localidad': 'location'
                                            })   
                                               
    zip_codes_file['postal_code'] = zip_codes_file['postal_code'].astype(int)

    #Read file with university data
    logger.info(f"Reading universities data file...")
    univ_data_file = pd.read_csv(f'{PATH}/files/UBA_data.txt', index_col = False)

    logger.info(f"Processing data...")

    #cast zip code as int so we can join
    univ_data_file['postal_code'] = univ_data_file['postal_code'].astype(int)

    #Join dataframes depending on which column we have in our original table
    if 'postal_code' in univ_data_file.columns:
        merged_data = pd.merge(univ_data_file, zip_codes_file, how = 'left', on = 'postal_code')
    elif 'location' in univ_data_file.columns:
        merged_data = pd.merge(univ_data_file, zip_codes_file, how = 'left', on = 'location')


    #processing string columns
    string_cols = ['university','career','full_name','gender','postal_code','location','email']
    
    sufix_prefix_dict = {'mr.': '','dr.': '','mrs.': '','ms.': '',
                            'md': '','dds': '','jr.': '','dvm': '','phd': ''}

    gender_dict = {"f": 'female',
                    "0": 'female',
                    "m": 'male',
                    "1": 'male'}

    for col in string_cols:
        merged_data[col] = (merged_data[col].astype('string')).str.strip().str.lower().str.replace('-',' ')
        if col == 'full_name':
            for fix, none in sufix_prefix_dict.items():
                merged_data[col] = merged_data[col].apply(lambda x: x.replace(fix, none))
    

    #Reformate dates
    merged_data['inscription_date'] = [datetime.strptime(x, '%y-%b-%d').strftime('%Y-%m-%d') for x in merged_data['inscription_date']]
    #Replace genders       
    merged_data = merged_data.replace({"gender": gender_dict})
    #Split name into first and last
    merged_data['first_name'] = merged_data['full_name'].str.split(' ', expand = True)[0]
    merged_data['last_name'] = merged_data['full_name'].str.split(' ', expand = True)[1]
    #Remove full name column
    merged_data.drop('full_name', axis = 1 , inplace = True)

    #Recalculate age
    merged_data['age'] = (np.where(merged_data['age'] < 0 , merged_data['age'] + 100, merged_data['age'])).astype('int')
 

    logger.info(f"Saving files...")
    #Reorder columns
    merged_data.reindex( columns=
                                ['university','career','inscription_date',
                                'first_name','last_name','gender','age',
                                'postal_code','location','email'])\
                                .to_csv(f'{PATH}/files/UBA_processed_data.txt', index = False)

def load_to_s3(file: str, key:str ,bucket: str):
    s3_hook = S3Hook('AWS_S3')
    s3_hook.load_file(filename=file,
                    key=key,
                    bucket_name=bucket)

    