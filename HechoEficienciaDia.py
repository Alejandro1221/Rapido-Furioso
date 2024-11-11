from connection_script import get_engine
import pandas as pd
from sqlalchemy import text

# Obtener la conexión a la base de datos ETL
conn_etl = get_engine('etl_rapidos_furiosos')

def etl_eficiencia_dia():
    # Obtener conexión a la base de datos ETL
    with conn_etl.connect() as conn:
        df_servicios = pd.read_sql("SELECT id_servicio, id_mensajero, id_estado, fecha_solicitud, duracion_total FROM servicios;", conn)
        df_novedades = pd.read_sql("SELECT id_novedad, id_mensajero, id_tipo_novedad, fecha_novedad FROM novedades;", conn)

        # Convertir fechas a formato datetime
        df_servicios['fecha_solicitud'] = pd.to_datetime(df_servicios['fecha_solicitud'], errors='coerce')
        df_novedades['fecha_novedad'] = pd.to_datetime(df_novedades['fecha_novedad'], errors='coerce')

        # Extraer el id_fecha para relacionar con la tabla de fechas
        df_servicios['id_fecha'] = df_servicios['fecha_solicitud'].apply(lambda x: obtener_id_fecha_fecha(conn, x))
        df_novedades['id_fecha'] = df_novedades['fecha_novedad'].apply(lambda x: obtener_id_fecha_fecha(conn, x))

    # Agrupar por id_mensajero, id_estado y fecha para calcular las métricas diarias
    servicios_agrupados = df_servicios.groupby(['id_mensajero', 'id_estado', 'id_fecha']).agg(
        cantidad_servicios=('id_servicio', 'size'),
        tiempo_promedio=('duracion_total', 'mean')
    ).reset_index()

    # Agrupar novedades para contar las reportadas por mensajero y fecha
    novedades_agrupadas = df_novedades.groupby(['id_mensajero', 'id_fecha']).size().reset_index(name='novedades_reportadas')

    # Unir las agrupaciones de servicios y novedades en un DataFrame para eficiencia_dia
    eficiencia_dia = pd.merge(servicios_agrupados, novedades_agrupadas, on=['id_mensajero', 'id_fecha'], how='left')

    # Reemplazar NaN en 'novedades_reportadas' por 0 en caso de no haber novedades reportadas en una fecha específica
    eficiencia_dia['novedades_reportadas'] = eficiencia_dia['novedades_reportadas'].fillna(0)

    # Seleccionar y ordenar las columnas finales para la tabla eficiencia_dia
    eficiencia_dia = eficiencia_dia[['id_estado', 'id_mensajero', 'id_fecha', 'cantidad_servicios', 'novedades_reportadas', 'tiempo_promedio']]

    # Cargar el DataFrame transformado a la tabla eficiencia_dia en la base de datos ETL
    try:
        eficiencia_dia.to_sql('eficiencia_dia', conn_etl, if_exists='append', index=False)
        print("ETL de eficiencia_dia completado exitosamente.")
    except Exception as e:
        print("Error al cargar los datos en la tabla eficiencia_dia:", e)

# Función para obtener el id_fecha de una fecha en la tabla fechas
def obtener_id_fecha_fecha(conn, fecha):
    query = text("SELECT id_fecha FROM fechas WHERE fecha = :fecha")
    
    # Asegúrate de manejar el caso donde fecha puede ser NaT (Not a Time)
    if pd.isna(fecha):
        return None
    
    result = conn.execute(query, {"fecha": fecha}).fetchone()
    
    return result[0] if result else None

# Ejecutar el ETL
etl_eficiencia_dia()