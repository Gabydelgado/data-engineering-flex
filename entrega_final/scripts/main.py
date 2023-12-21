import requests
import pandas as pd
from scripts import utils

from configparser import ConfigParser

def main(config):

    parser = ConfigParser()
    parser.read(config)
    url = 'https://api.coincap.io/v2/rates'
    response = requests.get(url)

    if response.status_code == 200:

        json_data = response.json()
        #Transforma datos en formato JSON en un DataFrame de Pandas.
        df = utils.tranforms_json(json_data)
        #Crea la conexión a la base de datos
        conn = utils.connect_db(parser)
        schema=parser['DB']['schema']
        #Carga datos en la base de datos desde un DataFrame.
        utils.load_data(df, conn, schema)
        print('Se cargaron los datos exitosamente')
        #Envía un email si la cripto X supera los Y dólares
        crypto = parser['ALERT']['crypto']
        value_alert = parser['ALERT']['value']
        email = parser['ALERT']['email']
        value_usd = df[df['cryptoName'] == crypto]['rateUsd'].values[0]
        if value_usd > int(value_alert):
            utils.send_email(crypto, value_usd, email)
    else:
        print('Error al llamar a la API')

if __name__ == '__main__':
    config = './config.ini'

    main(config)
