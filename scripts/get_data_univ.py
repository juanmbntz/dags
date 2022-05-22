import psycopg2
import pandas as pd
import pandas.io.sql as psql

def get_univ_data():

    #read file with zip codes
    url = 'https://drive.google.com/file/d/1or8pr7-XRVf5dIbRblSKlRmcP0wiP9QJ/view'
    path = 'https://drive.google.com/uc?export=download&id='+url.split('/')[-2]
    zip_codes = pd.read_csv(path)

    #set connection with postgres database
    connection = psycopg2.connect(
        host="52.4.33.38",
        dbname="training",
        user="alkymer",
        password="alkymer123")
    
    #create connection executable cursor
    cur = connection.cursor()

    #define the query we want to execute
    query = """
                SELECT 
                    universidad AS university,
                    carrera AS career,
                    fecha_de_inscripcion AS inscription_date,
                    SPLIT_PART(name, ' ', 1) AS first_name,
                    SPLIT_PART(name, ' ', 2) AS last_name,
                    sexo AS gender,
                    codigo_postal AS codigo_postal,
                    correo_electronico AS email

                FROM flores_comahue 
                WHERE universidad = 'UNIVERSIDAD DE FLORES'
          
                """

    #save the query output into a pandas dataframe
    df_universidad = psql.read_sql(query, connection)
    #cast zip code as intiger so data types match
    df_universidad['codigo_postal'] = df_universidad['codigo_postal'].astype(int)
    
    #left join using zip code
    final_table = pd.merge(df_universidad, zip_codes, how = 'left', on = 'codigo_postal')
    #rename column
    final_table.rename(columns = {'localidad' : 'location'}, inplace = True)

    #reorder columns
    final_table = final_table.reindex(
                                        columns=['university','career','inscription_date',
                                                'first_name','last_name','gender','age',
                                                'postal_code','location','email']
                                                  )
                                                  
    print(final_table.head(3))

    connection.close()