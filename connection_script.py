# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Cargar las variables del archivo .env
load_dotenv()

# Obtener las variables
password = os.getenv('PASSWORD')
user_db = os.getenv('USER_DB')
db_name_bodega = os.getenv('DB_NAME_BODEGA')
db_name_etl = os.getenv('DB_NAME_ETL')
db_port = os.getenv('DB_PORT')  # Obtener el puerto de la base de datos


# Configuración de conexión a la base de datos `bodega_datos`
DATABASE_URI_BODEGA = 'postgresql+psycopg2://{}:{}@localhost:5433/{}'.format(user_db, password, db_name_bodega)
engine_bodega = create_engine(DATABASE_URI_BODEGA)

# Configuración de conexión a la base de datos `etl_rapidos_furiosos`
DATABASE_URI_ETL = 'postgresql+psycopg2://{}:{}@localhost:5433/{}'.format(user_db, password, db_name_etl)
engine_etl = create_engine(DATABASE_URI_ETL)

# Función para obtener el engine de la base de datos solicitada
def get_engine(db_name):
    """Devuelve el engine correspondiente según el nombre de la base de datos"""
    if db_name == 'bodega_datos':
        return engine_bodega
    elif db_name == 'etl_rapidos_furiosos':
        return engine_etl
    else:
        raise ValueError(f"Base de datos {db_name} no configurada.")
    
# Si quieres probar la conexión a ambas bases de datos:
def connect_databases():
    """Prueba la conexión a ambas bases de datos."""
    try:
        with engine_bodega.connect() as connection:
            print("Conexión exitosa a la base de datos 'bodega_datos'")
    except Exception as e:
        print(f"Error al conectar a 'bodega_datos': {e}")

    try:
        with engine_etl.connect() as connection:
            print("Conexión exitosa a la base de datos 'etl_rapidos_furiosos'")
    except Exception as e:
        print(f"Error al conectar a 'etl_rapidos_furiosos': {e}")
