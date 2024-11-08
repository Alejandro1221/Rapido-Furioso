from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine
def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("""
            SELECT ciudad_id, nombre, departamento_id 
            FROM ciudad 
            WHERE ciudad_id IS NOT NULL AND nombre IS NOT NULL AND departamento_id IS NOT NULL
        """)
        result_bodega = conn_bodega.execute(query_bodega)
        ciudades = result_bodega.fetchall()
    return ciudades

def transform(ciudades, engine_etl):
    """Transformar los datos y validar existencia en la base de datos de destino."""
    valid_ciudades = []
    
    with engine_etl.connect() as conn_etl:
        for ciudad in ciudades:
            ciudad_id, nombre, departamento_id = ciudad
            
            # Validar si el departamento_id existe
            query_departamento = text("SELECT 1 FROM departamentos WHERE id_departamento = :departamento_id")
            result_departamento = conn_etl.execute(query_departamento, {"departamento_id": departamento_id}).fetchone()

            if not result_departamento:
                print(f"El departamento con ID {departamento_id} no existe. Ciudad {ciudad_id} no será insertada.")
                continue  # Saltar la inserción de esta ciudad
            
            # Agregar a la lista de ciudades válidas
            valid_ciudades.append((ciudad_id, nombre, departamento_id))
    
    return valid_ciudades

def load(valid_ciudades, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for ciudad in valid_ciudades:
            ciudad_id, nombre, departamento_id = ciudad
            
            # Verificar si el registro ya existe
            query_existente = text("SELECT 1 FROM ciudades WHERE id_ciudad = :ciudad_id")
            result_existente = conn_etl.execute(query_existente, {"ciudad_id": ciudad_id}).fetchone()

            if result_existente:
                print(f"La ciudad con ID {ciudad_id} ya existe, no se insertará.")
                continue
            
            # Insertar en la base de datos
            query_insert = text("""
                INSERT INTO ciudades (id_ciudad, nombre_ciudad, id_departamento) 
                VALUES (:ciudad_id, :nombre, :departamento_id)
            """)
            conn_etl.execute(query_insert, {"ciudad_id": ciudad_id, "nombre": nombre, "departamento_id": departamento_id})
        
        # Confirmar los cambios
        conn_etl.commit()

def transferir_ciudades():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    try:
        # Proceso ETL
        ciudades = extract(engine_bodega)
        valid_ciudades = transform(ciudades, engine_etl)
        load(valid_ciudades, engine_etl)

        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_ciudad, nombre_ciudad, id_departamento FROM ciudades")
            result_etl = conn_etl.execute(query_etl)
            ciudades_etl = result_etl.fetchall()

            # Convertir a DataFrame y mostrar
            df = pd.DataFrame(ciudades_etl, columns=["ID Ciudad", "Nombre Ciudad", "ID Departamento"])
            df.sort_values(by='ID Ciudad', inplace=True)
            df.reset_index(drop=True, inplace=True)

            print("Registros en la base de datos 'etl_rapidos_furiosos' (ciudades):")
            print(df)

    except Exception as e:
        print(f"Error al transferir las ciudades: {e}")

# Llamar a la función principal
if __name__ == "__main__":
    transferir_ciudades()