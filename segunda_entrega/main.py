import requests
import pandas as pd
import utils

from configparser import ConfigParser

config = ConfigParser()
config.read('./config.ini')

url = 'https://api.coincap.io/v2/rates'
response = requests.get(url)

if response.status_code == 200:

    json_data = response.json()
    #Transforma datos en formato JSON en un DataFrame de Pandas.
    df = utils.tranforms_json(json_data)
    #Crea la conexión a la base de datos
    conn = utils.connect_db(config)
    schema=config['DB']['schema']
    #Carga datos en la base de datos desde un DataFrame.
    utils.load_data(df, conn, schema)
    print('Se cargaron los datos exitosamente')
else:
    print('Error al llamar a la API')
