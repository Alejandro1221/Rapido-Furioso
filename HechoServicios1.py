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
               destino_id, COALESCE(
                NULLIF(mensajero3_id, NULL),  -- Si mensajero3_id no es nulo
                NULLIF(mensajero2_id, NULL),  -- Si mensajero2_id no es nulo
                NULLIF(mensajero_id, NULL)     -- Si mensajero_id no es nulo
            ) AS mensajero_id, origen_id, tipo_pago_id, tipo_servicio_id,
               tipo_vehiculo_id, usuario_id, prioridad, ciudad_destino_id,
               ciudad_origen_id, hora_visto_por_mensajero, visto_por_mensajero,
               descripcion_multiples_origenes, mensajero2_id, mensajero3_id,
               multiples_origenes, asignar_mensajero, es_prueba, descripcion_cancelado
        FROM mensajeria_servicio 
        ORDER BY id ASC;
        """
        df = pd.read_sql(query, conn_bodega)
        
    # Transformación
    with engine_etl.connect() as conn_etl:
        # Filtrar registros donde mensajero_id no sea null
        df = df[df['mensajero_id'].notnull()]
        
        # Obtener el mayor estado de cada servicio y asignarlo a la columna id_estado
        df['id_estado'] = df['id'].apply(lambda servicio_id: obtener_mayor_estado(engine_bodega, servicio_id))
       
        
        # Obtener fecha_entrega y hora_entrega para estado = 6 y tiempo de espera para estado = 2
        df['fecha_solicitud'], df['hora_solicitud'] = zip(*df['id'].apply(lambda x: obtener_fecha_hora_estado(engine_bodega, x, 1)))
        df['id_fecha_solicitud'] = df['fecha_solicitud'].apply(lambda x: obtener_id_fecha_fecha(conn_etl, x))

        df['fecha_mensajero_asignado'], df['hora_mensajero_asignado'] = zip(*df['id'].apply(lambda x: obtener_fecha_hora_estado(engine_bodega, x, 2)))
        df['id_fecha_mensajero_asignado'] = df['fecha_mensajero_asignado'].apply(lambda x: obtener_id_fecha_fecha(conn_etl, x))
        # print(f"Servicio ID: {df['id_fecha_mensajero_asignado']}")

        df["fecha_recogida"], df["hora_recogida"] = zip(*df["id"].apply(lambda x: obtener_fecha_hora_estado(engine_bodega, x, 4)))
        df["id_fecha_recogida"] = df["fecha_recogida"].apply(lambda x: obtener_id_fecha_fecha(conn_etl, x))
        
        df['fecha_entrega'], df['hora_entrega'] = zip(*df['id'].apply(lambda x: obtener_fecha_hora_estado(engine_bodega, x, 5)))
        df['id_fecha_entrega'] = df['fecha_deseada'].apply(lambda x: obtener_id_fecha_fecha(conn_etl, x))

        df['fecha_cerrado'], df['hora_cerrado'] = zip(*df['id'].apply(lambda x: obtener_fecha_hora_estado(engine_bodega, x, 6)))
        df['id_fecha_cerrado'] = df['fecha_cerrado'].apply(lambda x: obtener_id_fecha_fecha(conn_etl, x))
   
        # si fecha de mensajero asignado es nula, se asigna la fecha de solicitud
        df['fecha_mensajero_asignado'] = df['fecha_mensajero_asignado'].combine_first(df['fecha_solicitud'])
        df['hora_mensajero_asignado'] = df['hora_mensajero_asignado'].combine_first(df['hora_solicitud'])
        df['id_fecha_mensajero_asignado'] = df['id_fecha_mensajero_asignado'].combine_first(df['id_fecha_solicitud'])

        #si fecha de recolección es nula, se asigna la fecha de mensajero asignado
        df['fecha_recogida'] = df['fecha_recogida'].combine_first(df['fecha_mensajero_asignado'])
        df['hora_recogida'] = df['hora_recogida'].combine_first(df['hora_mensajero_asignado'])
        df['id_fecha_recogida'] = df['id_fecha_recogida'].combine_first(df['id_fecha_mensajero_asignado'])


        #si fecha de entrega es nula, se asigna la fecha de recolección
        df['fecha_entrega'] = df['fecha_entrega'].combine_first(df['fecha_recogida'])
        df['hora_entrega'] = df['hora_entrega'].combine_first(df['hora_recogida'])
        df['id_fecha_entrega'] = df['id_fecha_entrega'].combine_first(df['id_fecha_recogida'])

        #si fecha de cierre es nula, se asigna la fecha de entrega
        df['fecha_cerrado'] = df['fecha_cerrado'].combine_first(df['fecha_entrega'])
        df['hora_cerrado'] = df['hora_cerrado'].combine_first(df['hora_entrega'])
        df['id_fecha_cerrado'] = df['id_fecha_cerrado'].combine_first(df['id_fecha_entrega'])

        # Asignar id_fecha para fecha_solicitud y fecha_deseada desde la tabla fechas

        # Validar y mapear columnas necesarias para la base de datos ETL
        df['cliente_id'] = df['cliente_id'].apply(lambda x: validar_existencia_id(conn_etl, 'cliente', 'id_cliente', x))
        df['tipo_vehiculo_id'] = df['tipo_vehiculo_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_vehiculo', 'id_tipo_vehiculo', x))
        df['tipo_pago_id'] = df['tipo_pago_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_pago', 'id_tipo_pago', x))
        
        # Rellenar descripción_cancelado con "Sin información" si es nulo
        df['descripcion_cancelado'] = df['descripcion_cancelado'].fillna("Sin información")
        
        # Rellenar descripcion_pago con "Sin información de pago" si es nulo
        df['descripcion_pago'] = df['descripcion_pago'].fillna("Sin información de pago")
        
        # Calcular tiempos
        df['tiempo_espera'] = df.apply(lambda row: calcular_duracion(row['fecha_solicitud'], row['hora_solicitud'], row['fecha_mensajero_asignado'], row['hora_mensajero_asignado']), axis=1)
        df['tiempo_mensajero_recogida'] = df.apply(lambda row: calcular_duracion(row['fecha_mensajero_asignado'], row['hora_mensajero_asignado'], row['fecha_recogida'], row['hora_recogida']), axis=1)
        df['tiempo_recogida_entrega'] = df.apply(lambda row: calcular_duracion(row['fecha_recogida'], row['hora_recogida'], row['fecha_entrega'], row['hora_entrega']), axis=1)
        df['tiempo_entrega_cerrado'] = df.apply(lambda row: calcular_duracion(row['fecha_entrega'], row['hora_entrega'], row['fecha_cerrado'], row['hora_cerrado']), axis=1)

        df['duracion_total'] = df.apply(lambda row: calcular_duracion(row['fecha_solicitud'], row['hora_solicitud'], row['fecha_entrega'], row['hora_entrega']), axis=1)
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
            'descripcion_cancelado': 'descripcion_cancelado',
            'tipo_vehiculo_id':'id_tipo_vehiculo'
        }, inplace=True)
        
        # Filtrar las columnas finales para cargar a la tabla mayor
        columnas_finales = [
            'id_servicio', 
            'id_cliente', 
            'id_mensajero', 
            'id_fecha_solicitud', 
            'id_fecha_mensajero_asignado',
            'id_fecha_recogida',
            'id_fecha_entrega',
            'id_fecha_cerrado', 
            'hora_solicitud',
            'hora_mensajero_asignado', 
            'hora_recogida',
            'hora_entrega',
            'hora_cerrado',
            'fecha_solicitud',
            'fecha_entrega',
            'id_estado',
            'duracion_total', 
            'tiempo_espera',
            'tiempo_mensajero_recogida',
            'tiempo_recogida_entrega',
            'tiempo_entrega_cerrado', 
            'id_origen_servicio', 
            'id_destino_servicio', 
            'descripcion_pago', 
            'id_tiposervicio', 
            'prioridad', 
            'id_origen_ciudad', 
            'id_destino_ciudad', 
            'descripcion_cancelado',
            'id_tipo_vehiculo',
            'id_tipo_pago']
        
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

# Función para obtener el id_fecha de una fecha en la tabla fechas
def obtener_id_fecha_fecha(conn, fecha):
    query = text("SELECT id_fecha FROM fechas WHERE fecha = :fecha")
    result = conn.execute(query, {"fecha": fecha}).fetchone()
    
    # Si result es una tupla, usa el índice correcto
    return result[0] if result else None

# Función para obtener el mayor estado de un servicio dado
def obtener_mayor_estado(engine, servicio_id):
    """Obtiene el estado de mayor valor para un servicio específico."""
    query = text("""
        SELECT MAX(estado_id) as max_estado
        FROM mensajeria_estadosservicio
        WHERE servicio_id = :servicio_id
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"servicio_id": servicio_id}).fetchone()
    return result[0] if result else None


def obtener_fecha_hora_estado(engine, servicio_id, estado_id):
    """Obtiene la fecha y hora para un servicio dado y un estado específico."""
    query = text("""
        SELECT fecha, hora
        FROM mensajeria_estadosservicio
        WHERE servicio_id = :servicio_id AND estado_id = :estado_id
        ORDER BY fecha DESC, hora DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"servicio_id": servicio_id, "estado_id": estado_id}).fetchone()
    return result if result else (None, None)


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
    return (datetime_fin - datetime_inicio).total_seconds() / 3600  # Convertir a horas

# Ejecutar el ETL
etl_mayor()
