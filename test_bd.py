# test_bd.py

from connection_script import get_engine

# Conexión a la base de datos bodega_datos
engine_bodega_datos = get_engine('bodega_datos')

# Prueba de conexión, por ejemplo, obteniendo tablas disponibles
try:
    with engine_bodega_datos.connect() as conn:
        result = conn.execute("SELECT * FROM mensajeria_servicio")
        tables = result.fetchall()
        print("Tablas en bodega_datos:", tables)
except Exception as e:
    print(f"Error al conectar a 'bodega_datos': {e}")
