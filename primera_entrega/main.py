import pandas as pd
from datetime import datetime
import pytz
import requests

def tranforms_json(json_data):
    """
    Transforma datos en formato JSON en un DataFrame de Pandas.

    Args:
        json_data (dict): Un diccionario JSON que contiene 'data' y 'timestamp'.

    Returns:
        pandas.DataFrame: Un DataFrame de Pandas con los datos transformados.
    
    """

    data = json_data['data']
    timestamp = json_data['timestamp']
    zona_horaria = pytz.timezone('America/Argentina/Buenos_Aires')

    #Convierte el timestamp en una fecha en la zona horaria
    fecha = datetime.fromtimestamp(timestamp/1000, tz=zona_horaria).strftime('%Y-%m-%d %H:%M:%S')

    df = pd.DataFrame(data)
    df['updateTime'] = fecha

    #Convierte la columna 'updateTime' en un objeto de fecha y hora
    df['updateTime'] = pd.to_datetime(df['updateTime'])

    #Convierte la columna 'rateUsd' en tipo de dato float
    df['rateUsd'] = df['rateUsd'].astype(float)

    df.rename(columns={'id': 'cryptoName'}, inplace=True)

    return df

url = 'https://api.coincap.io/v2/rates'
response = requests.get(url)

if response.status_code == 200:
    json_data = response.json()
    df = tranforms_json(json_data)
    print(df.head(5))
else:
    print('Error al llamar a la API')