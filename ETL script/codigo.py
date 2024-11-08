pip install pandas sqlalchemy psycopg2


import pandas as pd
from sqlalchemy import create_engine

# Configura la conexión a la base de datos PostgreSQL
db_user = 'postgresql'
db_password = 'tu_contraseña'
db_host = 'localhost'  # Cambia si está en otro host
db_port = '5432'       # Puerto por defecto de PostgreSQL
db_name = 'nombre_base_datos'

# Crear el motor de conexión
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


def extraer_datos():
    # Ejemplo de extracción de datos de una tabla existente
    query = "SELECT * FROM servicios"
    df_servicios = pd.read_sql(query, engine)
    return df_servicios


def transformar_datos(df_servicios):
    # Ejemplo de transformación
    df_servicios['fecha_solicitud'] = pd.to_datetime(df_servicios['fecha_solicitud'])
    df_servicios['tiempo_entrega'] = (df_servicios['fecha_entrega'] - df_servicios['fecha_solicitud']).dt.total_seconds() / 3600  # en horas
    
    # Aquí puedes agregar más transformaciones según sea necesario
    return df_servicios


def cargar_datos(df_servicios):
    # Cargar los datos en la tabla de la bodega de datos
    df_servicios.to_sql('servicios', engine, if_exists='append', index=False)


def etl_rapido_furioso():
    # 1. Extraer datos
    df_servicios = extraer_datos()
    
    # 2. Transformar datos
    df_servicios_transformados = transformar_datos(df_servicios)
    
    # 3. Cargar datos
    cargar_datos(df_servicios_transformados)

# Ejecutar el ETL
if __name__ == '__main__':
    etl_rapido_furioso()
