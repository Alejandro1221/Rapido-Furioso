from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, DateTime, ForeignKey, MetaData
import pandas as pd
from connection_script import get_engine  # Asegúrate de tener el archivo `connection_script.py` con la función `get_engine`

# Obtener las conexiones a las bases de datos
engine_bodega = get_engine('bodega_datos')
engine_etl = get_engine('etl_rapidos_furiosos')

def extraer_datos_history_service():
    """Extrae los datos de la tabla history_service de la base de datos de la bodega."""
    with engine_bodega.connect() as conn_bodega:
        query = """
        SELECT id, fecha, hora, observaciones, estado_id, servicio_id
        FROM history_service
        """
        df_history_service = pd.read_sql(query, conn_bodega)
        print("Datos extraídos de la bodega de datos.")
        return df_history_service

def transformar_datos_history_service(df_history_service):
    """Filtra los datos para incluir solo los registros con servicio_id existentes en la tabla servicios de ETL."""
    with engine_etl.connect() as conn_etl:
        # Obtener los `servicio_id` existentes en la tabla `servicios`
        servicio_ids_existentes = pd.read_sql("SELECT id FROM servicios", conn_etl)['id'].tolist()
    
        # Filtrar los registros que tienen un `servicio_id` válido
        df_history_service_filtrado = df_history_service[df_history_service['servicio_id'].isin(servicio_ids_existentes)]
        print("Datos transformados para incluir solo los registros con servicio_id válidos.")
        return df_history_service_filtrado

def cargar_datos_history_service(df_history_service):
    """Carga los datos transformados a la tabla history_service en la base de datos ETL."""
    metadata = MetaData()
    tabla_history_service = Table(
        'history_service', metadata,
        Column('id', Integer, primary_key=True),
        Column('fecha', DateTime),
        Column('hora', String(10)),
        Column('observaciones', String(255)),
        Column('estado_id', Integer),
        Column('servicio_id', Integer, ForeignKey("servicios.id_servicio")),
    )

    with engine_etl.connect() as conn_etl:
        inspector = inspect(conn_etl)
        # Crear la tabla si no existe
        if not inspector.has_table('history_service'):
            metadata.create_all(conn_etl)
            print("Tabla 'history_service' creada en la base de datos ETL.")
        else:
            print("La tabla 'history_service' ya existe en la base de datos ETL.")
        
        # Cargar los datos transformados
        df_history_service.to_sql('history_service', conn_etl, if_exists='append', index=False)
        print("Datos cargados en la tabla 'history_service' en la base de datos ETL.")

# Ejecución principal del proceso ETL
if __name__ == "__main__":
    # Extracción
    datos_extraidos = extraer_datos_history_service()
    
    # Transformación
    datos_transformados = transformar_datos_history_service(datos_extraidos)
    
    # Carga
    cargar_datos_history_service(datos_transformados)
