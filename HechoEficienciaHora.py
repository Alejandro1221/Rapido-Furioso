import pandas as pd
from connection_script import get_engine
from sqlalchemy import text

# Obtener la conexión a la base de datos ETL
conn_etl = get_engine('etl_rapidos_furiosos')

def etl_eficiencia_hora():
    # Extraer los datos de las tablas necesarias
    with conn_etl.connect() as conn:
        df_servicios = pd.read_sql("SELECT id_servicio, id_mensajero, id_estado, fecha_solicitud, tiempo_total FROM servicios;", conn)
        df_novedades = pd.read_sql("SELECT id_novedad, id_mensajero, id_tipo_novedad, fecha_novedad FROM novedades;", conn)
        df_fechas = pd.read_sql("SELECT * FROM fechas;", conn)

    # Convertir fechas a formato datetime
    df_servicios['fecha_solicitud'] = pd.to_datetime(df_servicios['fecha_solicitud'], errors='coerce')
    df_novedades['fecha_novedad'] = pd.to_datetime(df_novedades['fecha_novedad'], errors='coerce')

    # Extraer año, mes, día para relacionar con la tabla de fechas
    df_servicios['id_fecha'] = df_servicios['fecha_solicitud'].dt.strftime('%Y%m%d').astype(int)
    df_novedades['id_fecha'] = df_novedades['fecha_novedad'].dt.strftime('%Y%m%d').astype(int)

    # Agrupar por id_mensajero, id_estado, id_fecha para calcular las métricas
    servicios_agrupados = df_servicios.groupby(['id_mensajero', 'id_estado', 'id_fecha']).agg(
        cantidad_servicios=('id_servicio', 'size'),           # Cantidad de servicios realizados
        tiempo_promedio=('tiempo_total', 'mean')              # Tiempo promedio de los servicios
    ).reset_index()

    # Agrupar novedades para contar las reportadas por mensajero y fecha
    novedades_agrupadas = df_novedades.groupby(['id_mensajero', 'id_fecha']).size().reset_index(name='novedades_reportadas')

    # Unir las agrupaciones de servicios y novedades en un DataFrame para `eficiencia_hora`
    eficiencia_hora = pd.merge(servicios_agrupados, novedades_agrupadas, on=['id_mensajero', 'id_fecha'], how='left')
    
    # Reemplazar NaN en 'novedades_reportadas' por 0 en caso de no haber novedades reportadas en una fecha específica
    eficiencia_hora['novedades_reportadas'] = eficiencia_hora['novedades_reportadas'].fillna(0)

    # Seleccionar y ordenar las columnas finales para la tabla `eficiencia_hora`
    eficiencia_hora = eficiencia_hora[['id_estado', 'id_mensajero', 'id_fecha', 'cantidad_servicios', 'novedades_reportadas', 'tiempo_promedio']]

    # Cargar el DataFrame transformado a la tabla `eficiencia_hora` en la base de datos ETL
    try:
        with conn_etl.connect() as conn_etl:
            eficiencia_hora.to_sql('eficiencia_hora', conn_etl, if_exists='append', index=False)
        print("ETL de eficiencia_hora completado exitosamente.")
    except Exception as e:
        print("Error al cargar los datos en la tabla eficiencia_hora:", e)

# Ejecutar el ETL
etl_eficiencia_hora()
