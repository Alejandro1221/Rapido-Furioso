from connection_script import get_engine
import pandas as pd
import numpy as np
from sqlalchemy import text
from pandas.errors import OutOfBoundsDatetime

# Obtener las conexiones a las bases de datos
engine_bodega = get_engine('bodega_datos')
engine_etl = get_engine('etl_rapidos_furiosos')

def etl_mayor():
    with engine_bodega.connect() as conn_bodega:
        # Extracción de los datos de la tabla mayor en la bodega de datos
        query = """
        SELECT id, descripcion, nombre_solicitante, fecha_solicitud, hora_solicitud,
               fecha_deseada, hora_deseada, nombre_recibe, telefono_recibe,
               descripcion_pago, ida_y_regreso, activo, novedades, cliente_id,
               destino_id, mensajero_id, origen_id, tipo_pago_id, tipo_servicio_id,
               tipo_vehiculo_id, usuario_id, prioridad, ciudad_destino_id,
               ciudad_origen_id, hora_visto_por_mensajero, visto_por_mensajero,
               descripcion_multiples_origenes, mensajero2_id, mensajero3_id,
               multiples_origenes, asignar_mensajero, es_prueba, descripcion_cancelado
        FROM mensajeria_servicio;
        """
        df = pd.read_sql(query, conn_bodega)
        
    # Transformación
    with engine_etl.connect() as conn_etl:
        # Filtrar registros donde mensajero_id no sea null
        df = df[df['mensajero_id'].notnull()]
        
        # Asignar rowid para fecha_solicitud y fecha_deseada desde la tabla fecha
        df['id_fecha_solicitud'] = df['fecha_solicitud'].apply(lambda x: obtener_rowid_fecha(conn_etl, x))
        df['id_fecha_entrega'] = df['fecha_deseada'].apply(lambda x: obtener_rowid_fecha(conn_etl, x))

        # Validar y mapear columnas necesarias para la base de datos ETL
        df['cliente_id'] = df['cliente_id'].apply(lambda x: validar_existencia_id(conn_etl, 'cliente', 'id_cliente', x))
        df['tipo_vehiculo_id'] = df['tipo_vehiculo_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_vehiculo', 'id_tipo_vehiculo', x))
        df['tipo_pago_id'] = df['tipo_pago_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_pago', 'id_tipo_pago', x))
        
        # Rellenar descripción_cancelado con "Sin información" si es nulo
        df['descripcion_cancelado'] = df['descripcion_cancelado'].fillna("Sin información")
        
        # Calcular duración total y tiempo de espera
        df['duracion_total'] = df.apply(lambda row: calcular_duracion(row['fecha_solicitud'], row['hora_solicitud'], row['fecha_deseada'], row['hora_deseada']), axis=1)
        df['tiempo_espera'] = df.apply(lambda row: calcular_tiempo_espera(row['fecha_solicitud'], row['fecha_deseada']), axis=1)
        
        # Renombrar columnas para cumplir con el esquema de la base de datos ETL
        df.rename(columns={
            'id': 'id_servicio',
            'cliente_id': 'id_cliente',
            'mensajero_id': 'id_mensajero',
            'destino_id': 'id_destino_servicio',
            'origen_id': 'id_origen_servicio',
            'tipo_pago_id': 'id_tipo_pago',
            'tipo_servicio_id': 'id_tiposervicio',
            'prioridad': 'prioridad',
            'ciudad_destino_id': 'id_destino_ciudad',
            'ciudad_origen_id': 'id_origen_ciudad',
            'descripcion_cancelado': 'descripcion_cancelado'
        }, inplace=True)
        
        # Filtrar las columnas finales para cargar a la tabla mayor
        columnas_finales = ['id_servicio', 'id_cliente', 'id_mensajero', 'id_fecha_solicitud',
                            'id_fecha_entrega', 'id_destino_servicio', 'id_origen_servicio', 
                            'descripcion_pago', 'id_tiposervicio', 'prioridad', 'id_destino_ciudad',
                            'id_origen_ciudad', 'duracion_total', 'tiempo_espera', 'descripcion_cancelado']
        
        df = df[columnas_finales]

    # Carga de los datos transformados a la tabla 'mayor' en la base de datos ETL
    with engine_etl.connect() as conn_etl:
        df.to_sql('servicios', conn_etl, if_exists='append', index=False)
        
    print("ETL completado exitosamente y datos cargados en la tabla 'mayor'.")

# Función de validación de existencia en tabla de referencia
def validar_existencia_id(conn, tabla, campo_id, valor_id):
    query = text(f"SELECT EXISTS(SELECT 1 FROM {tabla} WHERE {campo_id} = :valor_id)")
    result = conn.execute(query, {'valor_id': valor_id})
    return valor_id if result.scalar() else None

# Función para obtener el rowid de una fecha en la tabla fecha
def obtener_rowid_fecha(conn, fecha):
    query = text("SELECT rowid FROM fecha WHERE date_actual = :fecha")
    result = conn.execute(query, {'fecha': fecha})
    row = result.fetchone()
    return row['rowid'] if row else None

# Función para convertir fechas de manera segura
def safe_to_datetime(date_str):
    try:
        return pd.to_datetime(date_str, errors='coerce')
    except (OutOfBoundsDatetime, ValueError):
        return pd.NaT

# Función para calcular el tiempo de espera en horas entre dos fechas
def calcular_tiempo_espera(fecha_inicio, fecha_fin):
    datetime_inicio = safe_to_datetime(fecha_inicio)
    datetime_fin = safe_to_datetime(fecha_fin)
    if pd.isnull(datetime_inicio) or pd.isnull(datetime_fin):
        return np.nan  # Si alguna fecha es inválida, retornar NaN
    return (datetime_fin - datetime_inicio).total_seconds() / 3600  # Convertir a horas

# Función para calcular la duración total en horas entre dos fechas y horas dadas
def calcular_duracion(fecha_solicitud, hora_solicitud, fecha_deseada, hora_deseada):
    # Convierte las fechas y horas en objetos datetime
    try:
        datetime_inicio = pd.to_datetime(f"{fecha_solicitud} {hora_solicitud}")
        datetime_fin = pd.to_datetime(f"{fecha_deseada} {hora_deseada}")
    except (ValueError, TypeError):
        return np.nan  # Si la conversión falla, retornar NaN

    # Si alguna de las fechas es nula, retornar NaN
    if pd.isnull(datetime_inicio) or pd.isnull(datetime_fin):
        return np.nan

    # Calcula la duración en horas
    return (datetime_fin - datetime_inicio).total_seconds() / 3600  # Convierte a horas

# Ejecutar el ETL
etl_mayor()