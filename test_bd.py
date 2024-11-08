from connection_script import get_engine
from sqlalchemy import text

# Conexión a la base de datos bodega_datos
engine_bodega_datos = get_engine('bodega_datos')

try:
    # Establecer una conexión a la base de datos
    with engine_bodega_datos.connect() as conn:
        # Ejecutar la consulta sobre la conexión usando text()
        result = conn.execute(text("SELECT * FROM mensajeria_servicio"))
        
        # Obtener los resultados
        rows = result.fetchall()  # Almacena todos los resultados
        print("Datos de la tabla 'mensajeria_servicio':", rows)  # Imprimir resultados
        
except Exception as e:
    print(f"Error al conectar a 'bodega_datos': {e}")
