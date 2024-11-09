from connection_script import get_engine
import pandas as pd
import numpy as np
from sqlalchemy import text

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
        
        # Validar y mapear columnas necesarias para la base de datos ETL
        df['cliente_id'] = df['cliente_id'].apply(lambda x: validar_existencia_id(conn_etl, 'clientes', 'id_cliente', x))
        df['tipo_vehiculo_id'] = df['tipo_vehiculo_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_vehiculo', 'id', x))
        df['tipo_pago_id'] = df['tipo_pago_id'].apply(lambda x: validar_existencia_id(conn_etl, 'tipo_pago', 'id', x))
        
        # Rellenar descripción_cancelado con "Sin información" si es nulo
        df['descripcion_cancelado'].fillna("Sin información", inplace=True)
        
        # Calcular duración total y tiempo de espera
        df['duracion_total'] = df.apply(lambda row: calcular_duracion(row['fecha_solicitud'], row['hora_solicitud'], row['fecha_deseada'], row['hora_deseada']), axis=1)
        df['tiempo_espera'] = df.apply(lambda row: calcular_tiempo_espera(row['fecha_solicitud'], row['fecha_deseada']), axis=1)
        
        # Renombrar columnas para cumplir con el esquema de la base de datos ETL
        df.rename(columns={
            'id': 'id_servicio',
            'cliente_id': 'id_cliente',
            'mensajero_id': 'id_mensajero',
            'fecha_solicitud': 'id_fecha_solicitud',
            'fecha_deseada': 'id_fecha_entrega',
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
        df.to_sql('mayor', conn_etl, if_exists='append', index=False)
        
    print("ETL completado exitosamente y datos cargados en la tabla 'mensajeria_servicio'.")

# Función de validación de existencia en tabla de referencia
def validar_existencia_id(conn, tabla, campo_id, valor_id):
    query = text(f"SELECT EXISTS(SELECT 1 FROM {tabla} WHERE {campo_id} = :valor_id)")
    result = conn.execute(query, {'valor_id': valor_id})
    return valor_id if result.scalar() else None

# Función para calcular la duración total entre dos fechas y horas
def calcular_duracion(fecha_inicio, hora_inicio, fecha_fin, hora_fin):
    if pd.isnull(fecha_inicio) or pd.isnull(hora_inicio) or pd.isnull(fecha_fin) or pd.isnull(hora_fin):
        return np.nan
    datetime_inicio = pd.to_datetime(f"{fecha_inicio} {hora_inicio}")
    datetime_fin = pd.to_datetime(f"{fecha_fin} {hora_fin}")
    return (datetime_fin - datetime_inicio).total_seconds() / 3600

# Función para calcular el tiempo de espera entre dos fechas
def calcular_tiempo_espera(fecha_solicitud, fecha_deseada):
    if pd.isnull(fecha_solicitud) or pd.isnull(fecha_deseada):
        return np.nan
    return (pd.to_datetime(fecha_deseada) - pd.to_datetime(fecha_solicitud)).days

# Ejecutar el ETL
etl_mayor()
