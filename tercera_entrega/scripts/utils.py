import pandas as pd
from datetime import datetime
import pytz
import psycopg2

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

def connect_db(config):
    """
    Conecta a una base de datos Redshift.

    Returns:
        psycopg2.extensions.connection: Una conexión a la base de datos.
    
    """
    username = config['DB']['username']
    password = config['DB']['password']
    host = config['DB']['host']
    database = config['DB']['database']
    port = config['DB']['port']
    
    conn = psycopg2.connect(user=username, password=password, host=host, database=database, port=port)

    return conn

def load_data(df, conn, schema):
    """
    Carga datos en la base de datos desde un DataFrame.

    Parameters:
    - df (pd.DataFrame): DataFrame que contiene los datos a cargar.
    - conn (psycopg2.extensions.connection): Conexión a la base de datos.
    - schema (str): Nombre del esquema de la base de datos.

    Returns:
    - None: La función no devuelve ningún valor.
    """
    cursor = conn.cursor()

    # Preparar los datos para la inserción
    args = [tuple(x[1:]) for x in df.itertuples()]
    args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in args)

    # Ejecutar la inserción en la base de datos
    cursor.execute(f"INSERT INTO {schema}.crypto_currency (crypto_name, symbol, currency_symbol, type, rate_usd, update_time) VALUES {args_str};")

    # Confirmar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

    return None
