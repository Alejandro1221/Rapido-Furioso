from connection_script import get_engine
import pandas as pd
from sqlalchemy import text
from pandas.errors import OutOfBoundsDatetime

# Obtener las conexiones a las bases de datos
engine_bodega = get_engine('bodega_datos')
engine_etl = get_engine('etl_rapidos_furiosos')

def etl_novedades():
    with engine_bodega.connect() as conn_bodega:
        # Extracción de los datos de la tabla novedades en la bodega de datos
        query = """
        SELECT id, fecha_novedad, tipo_novedad_id, descripcion, servicio_id, es_prueba, mensajero_id
        FROM mensajeria_novedadesservicio;
        """
        df_novedades = pd.read_sql(query, conn_bodega)
        #print("Datos extraídos de la bodega de datos:", df_novedades.head())  # Verificar los datos extraídos

    # Transformación de los datos de novedades
    with engine_etl.connect() as conn_etl:
        # Validar y transformar datos según las necesidades del ETL
        df_novedades['servicio_id'] = df_novedades['servicio_id'].apply(lambda x: validar_existencia_id(conn_etl, 'servicios', 'id_servicio', x))
        df_novedades['descripcion'] = df_novedades['descripcion'].fillna("Sin descripción")

        # Renombrar columnas para la base de datos ETL
        df_novedades.rename(columns={
            'id':'id_novedad', 
            'fecha_novedad':'fecha_novedad', 
            'tipo_novedad_id':'id_tipo_novedad',
            'descripcion':'descripcion', 
            'servicio_id':'id_servicio', 
            'mensajero_id':'id_mensajero'
        }, inplace=True)

        # Filtrar las columnas finales para cargar a la tabla de novedades en la base de datos ETL
        columnas_finales_novedades = [
            'id_novedad', 
            'fecha_novedad', 
            'id_tipo_novedad', 
            'descripcion', 
            'id_servicio', 
            'id_mensajero'
        ]
        df_novedades = df_novedades[columnas_finales_novedades]

        # Verificar los datos transformados antes de la carga
        # print("Datos a cargar en ETL:", df_novedades.head())

        # Carga de los datos transformados a la tabla 'novedades' en la base de datos ETL
        try:
            with engine_etl.connect() as conn_etl:
                df_novedades.to_sql('novedades', conn_etl, if_exists='append', index=False)
                #df_novedades.to_sql('novedades', conn_etl, if_exists='append', index=False)
                print("ETL de novedades completado exitosamente.")
        except Exception as e:
            print("Error al cargar los datos en ETL:", e)

# Función de validación de existencia en tabla de referencia
def validar_existencia_id(conn, tabla, campo_id, valor_id):
    query = text(f"SELECT EXISTS(SELECT 1 FROM {tabla} WHERE {campo_id} = :valor_id)")
    result = conn.execute(query, {'valor_id': valor_id})
    return valor_id if result.scalar() else None

# Función para convertir fechas de manera segura
# def safe_to_datetime(date_str):
#     try:
#         return pd.to_datetime(date_str, errors='coerce')
#     except (OutOfBoundsDatetime, ValueError):
#         return pd.NaT
    
# Ejecutar el ETL
etl_novedades()
